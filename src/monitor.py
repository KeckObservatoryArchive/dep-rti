#!/kroot/rel/default/bin/kpython3
'''
Desc: Daemon to monitor for new FITS files to archive and and call DEP appropriately.
Run this per instrument for archiving new FITS files.  Will monitor KTL keywords
to find new files for archiving.  Keeps a queue for incoming filepaths and keeps
a list of spawned processes in order to manage how many concurrent processes can
run at once.

Usage: 
    python monitor.py [instr]

TODO:
- Add KTL monitoring
- Is log good enough to recover unprocessed files in event of monitor crash or
do we need something else?  Should we dump queue and procs lists to separate log?
'''
import sys
import argparse
import configparser
import datetime as dt
import time
import traceback
import os
import smtplib
from email.mime.text import MIMEText
import logging
import yaml
import db_conn
import importlib
from pathlib import Path
import subprocess
import threading
import multiprocessing
import ktl

from archive import Archive

import logging
log = logging.getLogger('koamonitor')


#Map needed keywords per instrument to standard key names
#todo: This json layout may need to be tweaked after we look at all the instruments.
#todo: This could be put in each of the instr subclasses.
instr_keys = {
    'KCWI': [
        {
            'service':   'kfcs',
            'lastfile':  'lastfile',
            'outdir':    'outdir',
            'outfile':   'outfile',
            'sequence':  'sequence'
        },
        {
            'service':   'kbds',
            'lastfile':  'loutfile',
            'outdir':    'outdir',
            'outfile':   'outfile',
            'sequence':  'frameno'
        }
    ],
    'NIRES': [
        {
            'service':   '???',
            'lastfile':  '???',
            'outdir':    '???',
            'outfile':   '???',
            'sequence':  '???'
        },
    ]
}


def main():

    # Define arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('instr', help='Keck Instrument')

    #parse args
    args = parser.parse_args()    
    instr = args.instr.upper()

    #run it and catch any unhandled error for email to admin
    try:
        monitor = Monitor(instr)
    except Exception as error:
        handle_fatal_error()


class Monitor():

    def __init__(self, instr):

        #input vars
        self.instr = instr

        #init other vars
        self.queue = []
        self.procs = []
        self.max_procs = 10

        #cd to script dir so relative paths work
        os.chdir(sys.path[0])

        #load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        #create logger first
        global log
        log = self.create_logger('koamonitor', self.config[instr]['ROOTDIR'], instr)
        log.info("Starting KOA Monitor: " + ' '.join(sys.argv[0:]))

        self.monitor()
        #self.monitor_test()


    def monitor(self):

        #run KTL monitor for each group defined for instr
        for keys in instr_keys[self.instr]:
            ktlmon = KtlMonitor(self.instr, keys, self)
            ktlmon.start()

        #start interval to monitor DEP processes for completion
        self.process_monitor()


    def monitor_test(self):

        #start internal time interval monitor of process list
        self.process_monitor()

        #add test files to queue every N seconds
        testfiles = ['/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0001.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0002.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0003.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0004.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0005.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0006.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0007.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0008.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0009.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0010.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0011.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0012.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0013.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/s200815_0014.fits',
                     '/Users/jriley/test/sdata/sdata1500/nires4/2020aug15/v200815_0001.fits']
        for f in testfiles:
            self.add_to_queue(f)
            time.sleep(0.1)


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        #Loop procs and remove from list if complete
        #NOTE: looping in reverse so we can delete without messing up looping
        log.debug(f"Checking processes. Size is {len(self.procs)}")
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                log.debug(f'---Removing completed process PID: {p.pid}')
                del self.procs[i]

        #check queue as well so we can add any jobs that were held up in the queue prior
        self.check_queue()

        #call this function every N seconds
        #todo: we could do this faster
        threading.Timer(1.0, self.process_monitor).start()


    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''

        #todo: test: change to test file for now
        filepath = '/usr/local/home/koarti/test/sdata/sdata1400/kcwi1/2020oct07/kf201007_000001.fits'

        #todo: change this to do database insert (check for duplicate).  
        #todo: The database will act as the queue and we will requery it to get next
        log.info(f'Adding to queue: {filepath}')
        self.queue.append(filepath)
        self.check_queue()


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        log.debug(f"Checking queue. Size is {len(self.queue)}")
        while len(self.queue) > 0:

            #check that we have not exceeded max num procs
            if len(self.procs) >= self.max_procs:
                log.warning(f'MAX {self.max_procs} concurrent processes exceeded. Queue size is {len(self.queue)}')
                break

            #pop from queue and process it
            filepath = self.queue.pop(0)
            self.process_file(filepath)


    def process_file(self, filepath):
        '''Spawn archiving for a single file.'''
        #NOTE: Using multiprocessing instead of subprocess so we can spawn loaded functions
        #as a separate process which saves us the ~0.5 second overhead of launching python.
        proc = multiprocessing.Process(target=self.spawn_processing, args=(self.instr, filepath))
        proc.start()
        self.procs.append(proc)
        log.debug(f'Started as process ID: {proc.pid}')


    def spawn_processing(self, instr, filepath):
        '''Call archiving for a single file.'''
        obj = Archive(self.instr, filepath=filepath, reprocess=True)


    def create_logger(self, name, rootdir, instr):
        """Creates a logger based on rootdir, instr and date"""

        # Create logger object
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        #paths
        processDir = f'{rootdir}/{instr.upper()}'
        logFile =  f'{processDir}/koa_monitor_{instr.upper()}.log'

        #create directory if it does not exist
        try:
            Path(processDir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"ERROR: Unable to create logger at {logFile}.  Error: {str(e)}")
            return False

        # Create a file handler
        handle = logging.FileHandler(logFile)
        handle.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        #add stdout to output so we don't need both log and print statements(>= warning only)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        #init message and return
        log.info(f'logger created for {name} at {logFile}')
        return log


class KtlMonitor():
    '''
    Class to handle monitoring a distinct set of keywords for an instrument to 
    determine when a new image has been written.
    '''

    def __init__(self, instr, keys, queue_mgr):
        log.info(f"KtlMonitor: instr: {instr}, service: {keys['service']}")
        self.instr = instr
        self.keys = keys
        self.queue_mgr = queue_mgr

    def start(self):
        '''Start monitoring 'lastfile' keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #create service object for easy reads later
            keys = self.keys
            self.service = ktl.cache(keys['service'])

            #monitor keyword that indicates new file
            kw = ktl.cache(keys['service'], keys['lastfile'])
            kw.callback(self.on_new_file)
            kw.monitor()

        except Exception as e:
            log.error("Could not start KTL monitoring.  Retrying in 60 seconds.")
            log.error(str(e))
            threading.Timer(60.0, self.start).start()
            return

    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''

        #todo: What is the best way to handle error/crashes in the callback?  Do we want the monitor to continue?
        #todo: Do we need to skip the initial read since that should be old? Can we check keyword time is old?
        try:
            if keyword['populated'] == False:
                log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            #get full file path
            #todo: For some instruments, we may need to form full path if lastfile is not defined.
            keys = self.keys
            outdir   = self.service[keys['outdir']]
            outfile  = self.service[keys['outfile']]
            sequence = self.service[keys['sequence']]
            lastfile = keyword.ascii

            #check for blank lastfile
            if not lastfile or not lastfile.strip():
                log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

            #send back to queue manager
            self.queue_mgr.add_to_queue(lastfile)

        except Exception as e:
            handle_fatal_error()


def handle_fatal_error():

    #form subject and msg (and log as well)
    subject = f'KOA ERROR: {sys.argv}'
    msg = traceback.format_exc()
    if log: log.error(subject + ' ' + msg)
    else: print(msg)

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    msg = MIMEText(msg)
    msg['Subject'] = subject
    msg['To']      = adminEmail
    msg['From']    = adminEmail
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


#--------------------------------------------------------------------------------
# main command line entry
#--------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
