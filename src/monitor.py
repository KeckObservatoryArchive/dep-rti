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

import dep

import logging
log = logging.getLogger('koamonitor')


def main():

    # Define inputs
    parser = argparse.ArgumentParser()
    parser.add_argument('instr', help='Keck Instrument')
    args = parser.parse_args()    

    #run it and catch any unhandled error for email to admin
    try:
        monitor = Monitor(args.instr)
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

        #self.monitor()
        self.monitor_test()


    def monitor():

        #start internal time interval monitor of process list
        self.process_monitor()

        #todo: monitor KTL keywords for new images and call DEP


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
            time.sleep(0.5)


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        #Loop procs and c
        #NOTE: looping in reverse so we can delete without messing up looping
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.poll() is not None:
                log.debug(f'---Removing completed process PID: {p.pid}')
                del self.procs[i]

        #check queue as well so we can add any jobs that were held up in the queue prior
        self.check_queue()

        #call this function every N seconds
        threading.Timer(1.0, self.process_monitor).start()


    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''
        log.info(f'Adding to queue: {filepath}')
        self.queue.append(filepath)
        self.check_queue()


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        while len(self.queue) > 0:

            #check that we have not exceeded max num procs
            if len(self.procs) >= self.max_procs:
                log.warning(f'MAX {self.max_procs} concurrent processes exceeded. Queue size is {len(self.queue)}')
                break

            #pop from queue and process it
            filepath = self.queue.pop(0)
            self.process_file(filepath)


    def process_file(self, filepath):
        '''Call archive for a single file.'''

        #create external command
        #TODO: Temp adding --reprocess flag for testing
        cmd = ('python', 'archive.py', self.instr, '--filepath', filepath, '--reprocess')
        log.info(f"Processing file with command: {' '.join(cmd)}")
        null = subprocess.DEVNULL
        proc = subprocess.Popen(cmd, stdin=null, stdout=null, stderr=null)
        self.procs.append(proc)
        log.debug(f'Started as process ID: {proc.pid}')


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
        sh.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        #init message and return
        log.info(f'logger created for {name} at {logFile}')
        return log


def handle_fatal_error():

    #form subject and msg (and log as well)
    subject = f'KOA ERROR: {sys.argv}'
    msg = traceback.format_exc()
    log.error(subject + ' ' + msg)

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


if __name__ == "__main__":
    main()
