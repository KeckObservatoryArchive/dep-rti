#!/kroot/rel/default/bin/kpython3
'''
Desc: Daemon to monitor for new FITS files and send to DEP for archiving.
Monitors KTL keywords to find new files for archiving.  Uses the database as its queue 
so the queue is not in memory.  Keeps a list of spawned processes to manage how many 
concurrent processes can run at once.  Can run as single or multiple instruments.

Usage: 
    python monitor.py [instr list]
    python monitor.py hires
    python monitor.py kcwi nires

Reference:
    http://spg.ucolick.org/KTLPython/index.html

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
import re
import hashlib

from archive import Archive
import monitor_config


#module globals
last_email_times = None
PROC_CHECK_SEC = 1.0
KTL_START_RETRY_SEC = 60.0
SERVICE_CHECK_SEC = 60.0


def main():
    '''Handle command line args and create one monitor object per instrument.'''

    # Arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('instruments', nargs='+', default=[], help='Keck instruments list to monitor')
    args = parser.parse_args()    

    #run monitors and catch any unhandled error for email to admin
    try:
        monitors = []
        for instr in args.instruments:
            monitor = Monitor(instr.upper())
            monitors.append(monitor)
    except Exception as error:
        handle_error('MONITOR_ERROR', traceback.format_exc())

    #stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
            for m in monitors:
                m.log.debug(f'Monitor saying hi every 5 minutes ({m.instr})')
        except:
            break
    for m in monitors:
        m.log.info(f'Exiting {__file__}')


class Monitor():
    '''
    Class to monitor KTL keywords for an instrument to find new files to archive.  
    When a new file is detected via KTL, will insert a record into DB.
    Monitors DB queue and spawns new DEP archive processes per datafile.
    '''
    def __init__(self, instr):

        #input vars
        self.instr = instr

        #init other vars
        self.queue = []
        self.procs = []
        self.max_procs = 10
        self.last_email_times = {}

        #cd to script dir so relative paths work
        os.chdir(sys.path[0])

        #load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        #create logger first
        self.log = self.create_logger(f'koa_monitor_{instr}', self.config[instr]['ROOTDIR'], instr)
        self.log.info(f"Starting KOA Monitor for {instr}")

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

        self.monitor()


    def __del__(self):

        #Close the database connection
        if self.db:
            self.db.close()


    def monitor(self):

        #run KTL monitor for each group defined for instr
        #NOTE: We must keep all our monitors in an array to prevent garbage collection
        self.monitors = []
        for keys in monitor_config.instr_keymap[self.instr]:
            ktlmon = KtlMonitor(self.instr, keys, self, self.log)
            ktlmon.start()
            self.monitors.append(ktlmon)

        #start interval to monitor DEP processes for completion
        self.process_monitor()


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        #Loop procs and remove from list if complete
        #NOTE: looping in reverse so we can delete without messing up looping
        removed = 0
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                self.log.debug(f'---Removing completed process PID={p.pid}, exitcode={p.exitcode}')
                del self.procs[i]
                removed += 1

        #If we removed any jobs, check queue 
        if removed: self.check_queue()

        #call this function every N seconds
        #NOTE: we could do this faster
        threading.Timer(PROC_CHECK_SEC, self.process_monitor).start()


    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''

        #Check if this is an exact duplicate file in name and contents
        try:
            if self.is_duplicate_file(filepath):
                return
        except Exception as e:
            self.log.error(traceback.format_exc())
            self.handle_error('DUPLICATE_FILE_CHECK_FAIL')

        #Do insert record
        self.log.info(f'Adding to queue: {filepath}')
        query = ("insert into dep_status set "
                f"   instrument='{self.instr}' "
                f" , ofname='{filepath}' "
                f" , status='QUEUED' "
                f" , creation_time='{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' ")
        self.log.info(query)
        result = self.db.query('koa', query)
        if result is False:
            self.handle_error('DATABASE_ERROR', query)
            return

        #check queue
        self.check_queue()


    def is_duplicate_file(self, filepath):
        '''
        Check dep_status for most recent record with same ofname.
        If not staged and (queued or processing) then it is definitely a duplicate.
        If staged and file contents/hash are same, the we will skip this file.
        NOTE: This is to get around unsolved duplicate trigger broadcast issue.
        '''
        q = ("select * from dep_status "
            f" where ofname='{filepath}' "
             " order by id desc limit 1")
        row = self.db.query('koa', q, getOne=True)
        if row is False:
            self.handle_error('DATABASE_ERROR', q)
            return False
        if len(row) == 0:
            return False
        stage_file = row['stage_file']
        status = row['status']

        #check for back to back duplicate broadcast (catch race condition)
        if not stage_file:
            if status in ('QUEUED', 'PROCESSING'):
                self.log.warning(f"Filepath '{filepath}' duplicate broadcast same as {row['id']}. Skipping.")
                return True            
            else:
                #If it is in some other state (invalid, error), we want to process the current one
                return False

        #check files exists (stage_file could be moved)
        if not os.path.isfile(stage_file) or not os.path.isfile(filepath):
            return False

        #compare md5s
        md5_stage = self.get_file_md5(stage_file)
        md5_new   = self.get_file_md5(filepath)
        if md5_stage == md5_new: 
            self.log.warning(f"Filepath '{filepath}' is same hash as staged_file for DB ID {row['id']}. Skipping.")
            return True
        else:
            return False


    def get_file_md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        query = (f"select * from dep_status where "
                f" status='QUEUED' "
                f" and instrument='{self.instr}' "
                f" order by creation_time asc limit 1")
        row = self.db.query('koa', query, getOne=True)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False
        if len(row) == 0:
            return 

        #check that we have not exceeded max num procs
        #todo: do we want to notify admins of this condition?
        if len(self.procs) >= self.max_procs:
            self.handle_error('MAX_PROCESSES', self.max_procs)
            return

        #set status to PROCESSING
        query = f"update dep_status set status='PROCESSING' where id={row['id']}"
        res = self.db.query('koa', query)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False

        #pop from queue and process it
        self.log.debug(f"Processing DB record ID={row['id']}, filepath={row['ofname']}")
        try:
            self.process_file(row['id'])
        except Exception as e:
            self.handle_error('PROCESS_ERROR', f"ID={row['id']}, filepath={row['ofname']}\n, {traceback.format_exc()}")


    def process_file(self, id):
        '''Spawn archiving for a single file by database ID.'''
        #NOTE: Using multiprocessing instead of subprocess so we can spawn loaded functions
        #as a separate process which saves us the ~0.5 second overhead of launching python.
        proc = multiprocessing.Process(target=self.spawn_processing, args=(self.instr, id))
        proc.start()
        self.procs.append(proc)
        self.log.debug(f'DEP started as system process ID: {proc.pid}')


    def spawn_processing(self, instr, dbid):
        '''Call archiving for a single file by DB ID.'''
        obj = Archive(self.instr, dbid=dbid, transfer=True)


    def create_logger(self, name, rootdir, instr):
        """Creates a logger based on rootdir, instr and date"""

        # Create logger object
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        #paths
        processDir = f'{rootdir}/{instr.upper()}'
        logFile =  f'{processDir}/{name}.log'

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
        log.info(f'logger created for {name} {instr} at {logFile}')
        return log

    def handle_error(self, errcode, text='', check_time=True):
        '''Email admins the error but only if we haven't sent one recently.'''

        #always log/print
        self.log.error(f'{errcode}: {text}')
        handle_error(errcode, text, self.instr, check_time)


class KtlMonitor():
    '''
    Class to handle monitoring a distinct keyword for an instrument to 
    determine when a new image has been written.

    Parameters:
        instr (str): Instrument to monitor.
        keys (dict): Defines service and keyword to monitor 
                     as well as special formatting to construct filepath.
        queue_mgr (obj): Class object that contains callback 'add_to_queue' function.
        log (obj): logger object
    '''
    def __init__(self, instr, keys, queue_mgr, log):
        self.log = log
        self.instr = instr
        self.keys = keys
        self.queue_mgr = queue_mgr
        self.service = None
        self.restart = False
        self.log.info(f"KtlMonitor: instr: {instr}, service: {keys['service']}, trigger: {keys['trigger']}")


    def start(self):
        '''Start monitoring 'trigger' keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #delete service if exists
            if self.service:
                del self.service
                self.service = None

            #create service object for easy reads later
            keys = self.keys
            self.service = ktl.Service(keys['service'])

            # monitor keys for services that construct filepath from other keywords
            filepath_keys = keys['fp_info']
            for key in filepath_keys:
                if key == keys['trigger']: continue
                kw = self.service[key]
                kw.monitor()

            #monitor keyword that indicates new file
            kw = self.service[keys['trigger']]
            kw.callback(self.on_new_file)

            # Prime callback to ensure it gets called at least once with current val
            if kw['monitored'] == True:
                self.on_new_file(kw)
            else:
                kw.monitor()

        except Exception as e:
            self.log.error(traceback.format_exc())
            msg = f"Could not start KTL monitoring for {self.instr} '{keys['service']}'.  Retry in 60 seconds."
            self.queue_mgr.handle_error('KTL_START_ERROR', msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return

        #Start an interval timer to periodically check that this service is running.
        threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()


    def check_service(self):
        '''
        Try to read heartbeat keyword from service.  If all ok, then check again in 1 minute.
        If we can't get a value, restart service monitoring.  
        '''
        heartbeat = self.keys.get('heartbeat')
        if not heartbeat: return

        try:
            val = self.service[heartbeat].read()
        except Exception as e:
            self.log.debug(f"KTL read exception: {str(e)}")
            val = None

        if not val:
            msg = f"KTL service {self.instr} '{self.keys['service']}' is NOT running.  Restarting service."
            self.queue_mgr.handle_error('KTL_CHECK_ERROR', msg)
            self.restart = True
            self.start()
        else:
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()


    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''
        try:
            self.log.debug(f'on_new_file: {keyword.name}={keyword.ascii}')

            if keyword['populated'] == False:
                self.log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            #assuming first read is old
            #NOTE: I don't think we could rely on a timestamp check vs now?
            if len(keyword.history) <= 1 or self.restart:
               self.log.info(f'Skipping first value read assuming it is old. Val is {keyword.ascii}')
               self.restart = False
               return

            #Get trigger val and if 'reqval' is defined make sure trigger equals reqval
            keys = self.keys
            reqval = keys['val']
            if reqval is not None and reqval != keyword.ascii:
                self.log.info(f'Trigger val of {keyword.ascii} != {reqval}')
                return

            # construct filepath from keywords(s) using lambda defined in monitor_config.py
            filepath_info = {}
            filepath_info[keyword.name] = keyword.ascii
            for key in keys['fp_info']:
                if key == keys['trigger']: continue
                kw = self.service[key]
                filepath_info[kw.name] = kw.ascii
                self.log.debug(f"\t{kw.name}={kw.ascii}")
            filepath = keys['format'](filepath_info)

            #check for blank filepath
            if not filepath or not filepath.strip():
                self.log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

            #Some filepaths do not add the /s/ to the path which we need
            if not filepath.startswith('/s/'):
                filepath = f'/s{filepath}' 

            #check for invalid filepath
            if '/sdata' not in filepath:
                self.log.error(f"INVALID FILEPATH (no 'sdata')\t{self.instr}\t{keyword.service}\t{filepath}")
                return

        except Exception as e:
            self.queue_mgr.handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager
        self.queue_mgr.add_to_queue(filepath)


def handle_error(errcode, text='', instr='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''
    #todo: Should last time be checked on a per instrument basis? (ie move this into class)

    #always log/print
    print(f'{errcode}: {text}')

    #Only send if we haven't sent one of same errcode recently
    if check_time:
        global last_email_times
        if not last_email_times: last_email_times = {}
        last_time = last_email_times.get(errcode)
        now = dt.datetime.now()
        if last_time and last_time + dt.timedelta(minutes=60) > now:
            return
        last_email_times[errcode] = now

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    body = f'{errcode}\n{text}'
    subj = f'KOA MONITOR ERROR: [{instr}] {errcode}'
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
