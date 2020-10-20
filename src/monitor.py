#!/kroot/rel/default/bin/kpython3
'''
Desc: Daemon to monitor for new FITS files to archive and call DEP appropriately.
Run this per instrument for archiving new FITS files.  Will monitor KTL keywords
to find new files for archiving.  Uses the database as its queue so the queue is not
in memory.  Keeps a list of spawned processes so as to manage how many concurrent 
processes can run at once.

Usage: 
    python monitor.py [instr]

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
import logging

from archive import Archive


#module globals
log = logging.getLogger('koamonitor')
last_email_times = None


#Define instrument keywords that indicate new datafile was written.
#todo: Finish mapping all instrs
#todo: This could be put in each of the instr subclasses.
instr_keys = {
    'KCWI': [
        {
            'service':   'kfcs',
            'lastfile':  'lastfile',
        },
        {
            'service':   'kbds',
            'lastfile':  'loutfile',
        }
    ],
    'NIRES': [
        {
            'service':   '???',
            'lastfile':  '???',
        },
    ],
    'DEIMOS': [],
    'ESI': [],
    'HIRES': [],
    'LRIS': [],
    'MOSFIRE': [],
    'NIRC2': [],
    'NIRSPEC': [],
    'OSIRIS': [],
}


def main():

    # Define arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('instr', help='Keck Instrument')

    #parse args
    args = parser.parse_args()    
    instr = args.instr.upper()

    #run monitor and catch any unhandled error for email to admin
    try:
        monitor = Monitor(instr)
    except Exception as error:
        email_error('MONITOR_ERROR', traceback.format_exc())


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

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

        self.monitor()
        #self.monitor_test()


    def __del__(self):

        #Close the database connection
        if self.db:
            self.db.close()


    def monitor(self):

        #run KTL monitor for each group defined for instr
        for keys in instr_keys[self.instr]:
            ktlmon = KtlMonitor(self.instr, keys, self)
            ktlmon.start()

        #start interval to monitor DEP processes for completion
        self.process_monitor()


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        self.log_periodic_hello()

        #Loop procs and remove from list if complete
        #NOTE: looping in reverse so we can delete without messing up looping
        #log.debug(f"Checking processes. Size is {len(self.procs)}")
        removed = 0
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                log.debug(f'---Removing completed process PID: {p.pid}')
                del self.procs[i]
                removed += 1

        #If we removed any jobs, check queue 
        if removed: self.check_queue()

        #call this function every N seconds
        #todo: we could do this faster
        threading.Timer(1.0, self.process_monitor).start()


    def log_periodic_hello(self):
        '''Log a hello message every hour so we know we are running ok.'''

        now = dt.datetime.now()
        if not hasattr(self, 'last_hello'):
            self.last_hello = now
        diff = now - self.last_hello
        if diff.seconds > 3600:
            log.debug('Monitor here, just saying hi every hour.')
            self.last_hello = now


    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''

        #todo: test: change to test file for now
        #filepath = '/usr/local/home/koarti/test/sdata/sdata1400/kcwi1/2020oct07/kf201007_000001.fits'

        ok = self.check_koa_db_entry(filepath)
        if not ok:
            email_error('DUPLICATE_FILEPATH', filepath)
            return

        #Do insert record
        log.info(f'Adding to queue: {filepath}')
        query = ("insert into dep_status set "
                f"   instr='{self.instr}' "
                f" , filepath='{filepath}' "
                f" , arch_stat='QUEUED' "
                f" , creation_time='{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' ")
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            self.email_error(f'{__name__} failed: {query}')
            return

        #check queue
        self.check_queue()


    def check_koa_db_entry(self, filepath):

        # See if entry exists
        query = (f"select count(*) as num from dep_status where "
                f" instr='{self.instr}' "
                f" and filepath='{filepath}' ")
        check = self.db.query('koa', query, getOne=True)
        if check is False:
            email_error('DATABASE_ERROR', f'{__name__}: Could not query {query}')
            return False
        if int(check['num']) > 0:
            return False
        return True


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        query = (f"select * from dep_status where "
                f" arch_stat='QUEUED' "
                f" and instr='{self.instr}' "
                f" order by creation_time desc limit 1")
        row = self.db.query('koa', query, getOne=True)
        if row is False:
            email_error('DATABASE_ERROR', f'{__name__}: Could not query: {query}')
            return False
        if len(row) == 0:
            return 

        #check that we have not exceeded max num procs
        if len(self.procs) >= self.max_procs:
            log.warning(f'MAX {self.max_procs} concurrent processes exceeded.')
            return

        #set status to PROCESSING
        query = f"update dep_status set arch_stat='PROCESSING' where id={row['id']}"
        res = self.db.query('koa', query)
        if row is False:
            email_error('DATABASE_ERROR', f'{__name__}: Could not query: {query}')
            return False

        #pop from queue and process it
        self.process_file(row['id'])


    def process_file(self, id):
        '''Spawn archiving for a single file by database ID.'''
        #NOTE: Using multiprocessing instead of subprocess so we can spawn loaded functions
        #as a separate process which saves us the ~0.5 second overhead of launching python.
        proc = multiprocessing.Process(target=self.spawn_processing, args=(self.instr, id))
        proc.start()
        self.procs.append(proc)
        log.debug(f'Started as process ID: {proc.pid}')


    def spawn_processing(self, instr, dbid):
        '''Call archiving for a single file by DB ID.'''
        obj = Archive(self.instr, dbid=dbid)


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
            email_error('KTL_START_ERROR', "Could not start KTL monitoring.  Retrying in 60 seconds.")
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
            lastfile = keyword.ascii

            #check for blank lastfile
            if not lastfile or not lastfile.strip():
                log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

        except Exception as e:
            email_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager
        self.queue_mgr.add_to_queue(lastfile)


def email_error(errcode, text, check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''

    #always log/print
    if log: log.error(f'{errcode}: {text}')
    else: print(text)

    #Only send if we haven't sent one of same errcode recently
    if check_time:
        global last_email_times
        if not last_email_times: last_email_times = {}
        last_time = last_email_times.get(errcode)
        now = dt.datetime.now()
        if last_time and last_time + dt.timedelta(hours=1) > now:
            return
        last_email_times[errcode] = now

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    body = f'{errcode}\n{text}'
    subj = f'KOA ERROR: ({os.path.basename(__file__)}) {errcode}'
    msg = MIMEText(body)
    msg['Subject'] = subj
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
