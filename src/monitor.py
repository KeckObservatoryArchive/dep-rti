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
import re

from archive import Archive


#module globals
last_email_times = None


#Define instrument keywords to monitor that indicate a new datafile was written.
#'trigger' value indicates which keyword to monitor that will trigger processing.
#If 'val' is defined, trigger must equal val to initiate processing.
#If 'format' is defined, all keywords in curlies will be replaced by that keyword value.
#If format is not defined, the value of the trigger will be used.
#If zfill is defined, then left pad those keyword vals (assuming with '0')
#todo: Finish mapping all instrs
#todo: This could be put in each of the instr subclasses.
instr_keys = {
    'KCWI': [
        {
            'service':  'kfcs',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"{vals['LASTFILE']}"
        },
        {
            'service':   'kbds',
            'trigger':  'LOUTFILE',
            'val'    :  None,
            'fp_info':  ['LOUTFILE'],
            'format' :  lambda vals: f"{vals['LOUTFILE']}"
        }
    ],
    'NIRES': [
        {
            'service':  'nids',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"{LASTFILE}"
        },
        {
            'service':  'nsds',
            'trigger':  'LASTFILE',
            'val'    :  None,
            'fp_info':  ['LASTFILE'],
            'format' :  lambda vals: f"/s{LASTFILE}"
        },
    ],
    'DEIMOS': [],
    'ESI': [],
    'HIRES': [
        {
            'service':  'hiccd',
            'trigger':  'WDISK',
            'val'    :  'false',
            'fp_info':  ['OUTDIR','OUTFILE','LFRAMENO'],
            'format' :  lambda vals: f"{vals['OUTDIR']}/{vals['OUTFILE']}{vals['LFRAMENO']:0>4}.fits"
        },
    ],
    'LRIS': [],
    'MOSFIRE': [],
    'NIRC2': [],
    'NIRSPEC': [],
    'OSIRIS': [],
}


def main():

    # Define arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('instruments', nargs='+', default=[], help='Keck instruments list to monitor')

    #parse args
    args = parser.parse_args()    

    #run monitor and catch any unhandled error for email to admin
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
                m.log.debug('Monitor here just saying hi every 5 minutes')
        except:
            break
    for m in monitors:
        m.log.info(f'Exiting {__file__}')


class Monitor():
    '''
    Class to monitor all keywords that indicate a new file is written.  
    When a new file is detected, will insert a record into DB.
    Monitors DB queue and spawns new DEP archive processes per datafile.
    '''
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
        for keys in instr_keys[self.instr]:
            ktlmon = KtlMonitor(self.instr, keys, self, self.log)
            ktlmon.start()
            self.monitors.append(ktlmon)

        #start interval to monitor DEP processes for completion
        self.process_monitor()


    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        #Loop procs and remove from list if complete
        #NOTE: looping in reverse so we can delete without messing up looping
        #self.log.debug(f"Checking processes. Size is {len(self.procs)}")
        removed = 0
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                self.log.debug(f'---Removing completed process PID: {p.pid}')
                del self.procs[i]
                removed += 1

        #If we removed any jobs, check queue 
        if removed: self.check_queue()

        #call this function every N seconds
        #todo: we could do this faster
        threading.Timer(1.0, self.process_monitor).start()


    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''

        #todo: test: change to test file for now
        #filepath = '/usr/local/home/koarti/test/sdata/sdata1400/kcwi1/2020oct07/kf201007_000001.fits'

        ok = self.check_koa_db_entry(filepath)
        if not ok:
            handle_error('DUPLICATE_FILEPATH', filepath)
            return

        #Do insert record
        self.log.info(f'Adding to queue: {filepath}')
        query = ("insert into dep_status set "
                f"   instrument='{self.instr}' "
                f" , ofname='{filepath}' "
                f" , status='QUEUED' "
                f" , creation_time=NOW() ")
        self.log.info(query)
        result = self.db.query('koa', query)
        if result is False:
            handle_error('QUEUE_INSERT_ERROR', f'{__name__} failed: {query}')
            return

        #check queue
        self.check_queue()


    def check_koa_db_entry(self, filepath):

        # See if entry exists
        query = (f"select count(*) as num from dep_status where "
                f" instrument='{self.instr}' "
                f" and ofname='{filepath}' ")
        check = self.db.query('koa', query, getOne=True)
        if check is False:
            handle_error('DATABASE_ERROR', f'{__name__}: Could not query {query}')
            return False
        if int(check['num']) > 0:
            return False
        return True


    def check_queue(self):
        '''Check queue for jobs that need to be spawned.'''
        query = (f"select * from dep_status where "
                f" status='QUEUED' "
                f" and instrument='{self.instr}' "
                f" order by creation_time desc limit 1")
        row = self.db.query('koa', query, getOne=True)
        if row is False:
            handle_error('DATABASE_ERROR', f'{__name__}: Could not query: {query}')
            return False
        if len(row) == 0:
            return 

        #check that we have not exceeded max num procs
        if len(self.procs) >= self.max_procs:
            self.log.warning(f'MAX {self.max_procs} concurrent processes exceeded.')
            return

        #set status to PROCESSING
        query = f"update dep_status set status='PROCESSING' where id={row['id']}"
        res = self.db.query('koa', query)
        if row is False:
            handle_error('DATABASE_ERROR', f'{__name__}: Could not query: {query}')
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
        self.log.debug(f'Started as process ID: {proc.pid}')


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
        self.log.info(f"KtlMonitor: instr: {instr}, service: {keys['service']}, trigger: {keys['trigger']}")

    def start(self):
        '''Start monitoring 'trigger' keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #create service object for easy reads later
            keys = self.keys
            filepath_keys = keys['fp_info']

            self.service = ktl.cache(keys['service'])

            # monitor keys for services that don't have a lastfile equivalent
            for key in filepath_keys:
                keyword = ktl.cache(keys['service'], key)
                keyword.monitor()

            #monitor keyword that indicates new file
            kw = ktl.cache(keys['service'], keys['trigger'])
            kw.callback(self.on_new_file)

            # Prime callback to ensure it gets called at least once with current val
            if kw['monitored'] == True:
                self.on_new_file(kw)
            else:
                kw.monitor()

        except Exception as e:
            handle_error('KTL_START_ERROR', "Could not start KTL monitoring.  Retrying in 60 seconds.")
            threading.Timer(60.0, self.start).start()
            return


    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''
        keys = self.keys
        reqval = keys['val']
        try:
            if keyword['populated'] == False:
                self.log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            #assuming first read is old
            #NOTE: I don't think we could rely on a timestamp check vs now?
            if len(keyword.history) <= 1:
                self.log.info(f'Skipping first value read assuming it is old. Val is {keyword.ascii}')
                return

            if reqval is None or keyword.ascii==reqval:
                # construct the filepath from the keywords(s)
                filepath_keys = keys['fp_info']
                filepath_info = {}

                for key in filepath_keys:
                    keyword = self.service[key]
                    filepath_info[keyword.name] = keyword.ascii

                filepath = keys['format'](filepath_info)

                #check for blank filepath
                if not filepath or not filepath.strip():
                    self.log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                    return
            else:
                self.log.info(f'Trigger val of {trigger} != {reqval}')
                return

        except Exception as e:
            handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager
        self.queue_mgr.add_to_queue(filepath)


    def get_formatted_filepath(self, format, zfill):
        '''
        Construct filepath from multiple KTL keywords. See instr_keys module global defined above.
        Parameters:
            format (str): Path formatting containing keywords in curlies to replace with KTL values
            zfill (dict): Map KTL keywords to '0' zfill.
        '''
        filepath = format
        matches = re.findall("{.*?}", format)
        for key in matches:
            key = key[1:-1]
            val = self.service[key].read()
            pad = zfill.get(key, None)
            if pad is not None:
                val = val.zfill(pad)
            filepath = filepath.replace('{'+key+'}', val)
        return filepath


def handle_error(errcode, text=None, check_time=True):
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
