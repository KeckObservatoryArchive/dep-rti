#!/usr/local/anaconda/bin/python
'''
Desc: Per-instrument daemon to monitor koa_status records for completed DRP processes that are 
ready to archive.

The DRP will call the ingestAPI to tell us that a KOAID is ready to be archived.  The API will 
create the QUEUED entry.  The monitor will start the process to collect the data, create the meta 
files and trigger ingestion.

Usage: 
    python monitor.py [instrument]
    python monitor.py kcwi

'''
import sys
import argparse
import datetime as dt
import time
import traceback
import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
import threading
import multiprocessing
from common import create_logger, get_config

from db_conn import db_conn
from archive import Archive


#module globals
last_email_times = None



def main():
    '''Handle command line args and create monitor object for instrument.'''

    # Arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('instr', help='Instrument to monitor.')
    args = parser.parse_args()    

    #run monitors and catch any unhandled error for email to admin
    try:
        monitor = Monitor(args.instr)
    except Exception as err:
        handle_error('MONITOR_ERROR', traceback.format_exc())
        sys.exit(1)

    #stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
            monitor.log.debug(f'Monitor saying hi every 5 minutes ({monitor.instr})')
        except:
            break
    monitor.log.info(f'Exiting {__file__}')


class Monitor():
    '''
    Class to monitor koa_status records for a particular instrument to spawn DRP processes.
    Monitors DB queue and spawns new DRP archive processes per datafile.
    '''
    def __init__(self, instr):

        #input vars
        self.instr = instr.upper()

        #init other vars
        self.queue = []
        self.procs = []
        self.last_queue_check = time.time()
        self.last_email_times = {}
        self.db = None

        #cd to script dir so relative paths work
        os.chdir(sys.path[0])

        #load config file
        self.config = get_config()

        #create logger first
        self.logger = self.create_drp_logger(self.config[self.instr]['ROOTDIR'], self.instr)
        self.logger.info(f"Starting KOA DRP Monitor for {self.instr}")

        # Establish database connection 
        self.db = db_conn(persist=True)

        self.start()


    def __del__(self):

        #Close the database connection
        if self.db:
            self.db.close()


    def start(self):

        #start DB and process monitors
        self.process_monitor()
        self.queue_monitor(True)


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        #Loop procs and remove from list if complete
        #NOTE: looping in reverse so we can delete without messing up looping
        removed = 0
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                self.logger.debug(f'---Removing completed process PID={p.pid}, exitcode={p.exitcode}')
                del self.procs[i]
                removed += 1

        #If we removed any jobs, check queue 
        if removed: self.check_queue()

        #call this function every N seconds
        #NOTE: we could do this faster
        threading.Timer(self.config['MONITOR_DRP']['PROC_CHECK_SEC'], self.process_monitor).start()


    def queue_monitor(self, init=False):
        '''
        Periodically check the queue when idle.
        NOTE: Queue is re-checked when an entry is made in the queue or if
        a job finishes.  However, if an entry is manually entered in queue
        outside of nominal operation, this will pick it up.
        '''
        now = time.time()
        diff = int(now - self.last_queue_check)
        if diff >= self.config['MONITOR_DRP']['QUEUE_CHECK_SEC'] or init:
            self.check_queue()

        #call this function every N seconds
        threading.Timer(self.config['MONITOR_DRP']['QUEUE_CHECK_SEC'], self.queue_monitor).start()


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        self.last_queue_check = time.time()

        query = (f"select * from koa_status where level in (1,2) "
                f" and status='QUEUED' "
                f" and instrument='{self.instr}' "
                f" order by creation_time asc limit 1")

        row = self.db.query('koa', query, getOne=True)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False
        if len(row) == 0:
            return 

        #check that we have not exceeded max num procs
        if len(self.procs) >= self.config['MONTIOR_DRP']['MAX_PROCESSES']:
#            self.handle_error('MAX_PROCESSES', MAX_PROCESSES)
            return

        #set status to PROCESSING
        query = f"update koa_status set status='PROCESSING' where id={row['id']}"
        res = self.db.query('koa', query)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False

        #pop from queue and process it
        self.logger.debug(f"Processing DB record ID={row['id']}, filepath={row['ofname']}")
        try:
            self.process_file(row['id'], row['level'])
        except Exception as err:
            self.handle_error('PROCESS_ERROR', f"ID={row['id']}, filepath={row['ofname']}\n, {traceback.format_exc()}")


    def process_file(self, id, level):
        '''Spawn archiving for a single file by database ID.'''
        #NOTE: Using multiprocessing instead of subprocess so we can spawn loaded functions
        #as a separate process which saves us the ~0.5 second overhead of launching python.
        proc = multiprocessing.Process(target=self.spawn_processing, args=(id, level))
        proc.start()
        self.procs.append(proc)
        self.logger.debug(f'DEP started as system process ID: {proc.pid}')


    def spawn_processing(self, dbid, level):
        '''Call archiving for a single file by DB ID.'''
#        koaxfr = True if level == 1 else False
        obj = Archive(self.instr, dbid=dbid, transfer=True)
        pass


    def create_drp_logger(self, rootdir, instr):
        """Creates a logger based on rootdir, instr and date"""

        # Create logger object
        name = f'koa.monitor.drp.{instr}'.lower()
        #paths
        processDir = f'{rootdir}/{instr.upper()}'
        logFile =  f'{processDir}/{name.replace(".","_")}.log'

        #create directory if it does not exist
        try:
            Path(processDir).mkdir(parents=True, exist_ok=True)
        except Exception as err:
            print(f"ERROR: Unable to create logger at {logFile}.  Error: {str(err)}")
            return False

        # Create a file handler
        logger = create_logger(name, logFile)
        return logger


    def handle_error(self, errcode, text='', check_time=True):
        '''Email admins the error but only if we haven't sent one recently.'''

        #always log/print
        self.logger.error(f'{errcode}: {text}')
        handle_error(errcode, text, self.instr, check_time)



def handle_error(errcode, text='', instr='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''

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
    config = get_config()
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    body = f'{errcode}\n{text}'
    subj = f'KOA DRP MONITOR ERROR: [{instr}] {errcode}'
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
