#!/kroot/rel/default/bin/kpython3
'''
Desc: Daemon to monitor for new FITS files and send to DEP for archiving.
Monitors KTL keywords to find new files for archiving.  Uses the database as its queue 
so the queue is not in memory.  Keeps a list of spawned processes to manage how many 
concurrent processes can run at once.  Run per instrument service.

Usage: 
    python monitor.py [service name]
    python monitor.py kfcs

Reference:
    http://spg.ucolick.org/KTLPython/index.html

'''
import sys
import argparse
import datetime as dt
import time
import traceback
import os
import smtplib
from email.mime.text import MIMEText
import yaml
from pathlib import Path
import threading
import multiprocessing
import ktl
import logging
import re
import hashlib

from archive import Archive
import monitor_config
import db_conn

# module globals
last_email_times = None
PROC_CHECK_SEC = 1.0
KTL_START_RETRY_SEC = 60.0
SERVICE_CHECK_SEC = 60.0
QUEUE_CHECK_SEC = 60.0
EMAIL_INTERVAL_MINUTES = 60


def main():
    '''Handle command line args and create monitor object for service.'''

    # Arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', help='The name of the instrument mode to monitor.')
    args = parser.parse_args()    

    # run monitors and catch any unhandled error for email to admin
    try:
        monitor = Monitor(args.mode)
    except Exception as error:
        handle_error('MONITOR_ERROR', traceback.format_exc(), service=args.mode)
        sys.exit(1)

    # stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
            monitor.log.debug(f'Monitor saying hi every 5 minutes '
                              f'({monitor.instr} {monitor.service})')
        except:
            break
    monitor.log.info(f'Exiting {__file__}')


class Monitor:
    '''
    Class to monitor KTL service to find new files to archive.  
    When a new file is detected via KTL, will insert a record into DB.
    Monitors DB queue and spawns new DEP archive processes per datafile.
    '''
    def __init__(self, inst_mode_name):

        # init other vars
        self.queue = []
        self.procs = []
        self.max_procs = 10
        self.last_queue_check = None
        self.last_email_times = {}
        self.db = None

        # cd to script dir so relative paths work
        os.chdir(sys.path[0])

        # load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE',
                                  persist=True)

        # get ktl-service-name and instrument from the name of instrument + mode
        try:
            self.keys = monitor_config.instr_keymap[inst_mode_name]
            self.service_name = self.keys['ktl_service']
            self.instr = self.keys['instr']
        except KeyError:
            err = f"Instrument name: {inst_mode_name}, " \
                  f"{inst_mode_name}.ktl_service, and " \
                  f"{inst_mode_name}.instr must be defined in monitor_config.py"
            handle_error('CONFIG_ERROR', text=err)
            sys.exit(1)

        self.transfer = self.keys.get('transfer', False)

        # create logger first
        self.log = self.create_logger(self.config[self.instr]['ROOTDIR'],
                                      self.instr, self.service_name)
        self.log.info(f"Starting KOA Monitor for {self.instr} "
                      f"{self.service_name}")

        self.start_monitor()

    def __del__(self):

        # Close the database connection
        if self.db:
            self.db.close()

    def start_monitor(self):
        # run KTL monitor for service
        monitor = KtlMonitor(self.service_name, self.keys, self, self.log)
        monitor.start()

        # start interval to monitor DEP processes for completion
        self.process_monitor()
        self.queue_monitor()

    def process_monitor(self):
        '''Remove any processes from list that are complete.'''

        # Loop procs and remove from list if complete
        # NOTE: looping in reverse so we can delete without messing up looping
        removed = 0
        for i in reversed(range(len(self.procs))):
            p = self.procs[i]
            if p.exitcode is not None:
                self.log.debug(f'---Removing completed process PID={p.pid}, '
                               f'exitcode={p.exitcode}')
                del self.procs[i]
                removed += 1

        # If we removed any jobs, check queue
        if removed:
            self.check_queue()

        # call this function every N seconds
        # NOTE: we could do this faster
        threading.Timer(PROC_CHECK_SEC, self.process_monitor).start()

    def add_to_queue(self, filepath):
        '''Add a file to queue for processing'''

        # Check if this is an exact duplicate file in name and contents
        try:
            if self.is_duplicate_file(filepath):
                return
        except Exception as e:
            self.log.error(traceback.format_exc())
            self.handle_error('DUPLICATE_FILE_CHECK_FAIL')

        # Do insert record
        self.log.info(f'Adding to queue: {filepath}')
        now = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        query = ("insert into koa_status set level=0,"
                f"   instrument='{self.instr}' "
                f" , service='{self.service_name}' "
                f" , ofname='{filepath}' "
                f" , status='QUEUED' "
                f" , creation_time='{now}' ")
        self.log.info(query)
        result = self.db.query('koa', query)
        if result is False:
            self.handle_error('DATABASE_ERROR', query)
            return

        # check queue
        self.check_queue()

    def is_duplicate_file(self, filepath):
        '''
        Check koa_status for most recent record with same ofname.
        If not staged and (queued or processing) then it is definitely a duplicate.
        If staged and file contents/hash are same, the we will skip this file.
        NOTE: This is to get around unsolved duplicate trigger broadcast issue.
        '''
        q = ("select * from koa_status "
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

        # check for back to back duplicate broadcast (catch race condition)
        if not stage_file:
            if status in ('QUEUED', 'PROCESSING', 'TRANSFERRING', 'TRANSFERRED'):
                self.log.warning(f"Filepath '{filepath}' duplicate "
                                 f"broadcast same as {row['id']}. Skipping.")
                return True            
            else:
                # If it is in some other state (invalid, error), we want to process the current one
                return False

        # check files exists (stage_file could be moved)
        if not os.path.isfile(stage_file) or not os.path.isfile(filepath):
            return False

        # compare md5s
        md5_stage = self.get_file_md5(stage_file)
        md5_new   = self.get_file_md5(filepath)
        if md5_stage == md5_new: 
            self.log.warning(f"Filepath '{filepath}' is same hash as "
                             f"staged_file for DB ID {row['id']}. Skipping.")
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
        self.last_queue_check = time.time()

        query = (f"select * from koa_status where level=0 "
                f" and status='QUEUED' "
                f" and instrument='{self.instr}' "
                f" and service='{self.service_name}' "
                f" order by creation_time asc limit 1")
        row = self.db.query('koa', query, getOne=True)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False
        if len(row) == 0:
            return 

        # check that we have not exceeded max num procs
        if len(self.procs) >= self.max_procs:
            self.handle_error('MAX_PROCESSES', self.max_procs)
            return

        # set status to PROCESSING
        query = f"update koa_status set status='PROCESSING' where id={row['id']}"
        res = self.db.query('koa', query)
        if row is False:
            self.handle_error('DATABASE_ERROR', query)
            return False

        # pop from queue and process it
        self.log.debug(f"Processing DB record ID={row['id']}, "
                       f"filepath={row['ofname']}")
        try:
            self.process_file(self.instr, row['id'])
        except Exception as e:
            self.handle_error('PROCESS_ERROR',
                              f"ID={row['id']}, filepath={row['ofname']}\n, "
                              f"{traceback.format_exc()}")

    def queue_monitor(self):
        '''
        Periodically check the queue when idle.
        NOTE: Queue is re-checked when an entry is made in the queue or if
        a job finishes.  However, if an entry is manually entered in queue
        outside of nominal operation, this will pick it up.
        '''
        now = time.time()
        diff = int(now - self.last_queue_check) if self.last_queue_check else 0
        if diff >= QUEUE_CHECK_SEC or not self.last_queue_check:
            self.check_queue()

        # call this function every N seconds
        threading.Timer(QUEUE_CHECK_SEC, self.queue_monitor).start()

    def process_file(self, instr, id):
        '''Spawn archiving for a single file by database ID.'''
        # NOTE: Using multiprocessing instead of subprocess so we can spawn loaded functions
        # as a separate process which saves us the ~0.5 second overhead of launching python.

        proc = multiprocessing.Process(target=self.spawn_processing, args=(self.instr, id))
        proc.start()
        self.procs.append(proc)
        self.log.debug(f'DEP started as system process ID: {proc.pid}')

    def spawn_processing(self, instr, dbid):
        '''Call archiving for a single file by DB ID.'''
        obj = Archive(self.instr, dbid=dbid, transfer=self.transfer)

    def create_logger(self, rootdir, instr, service):
        """Creates a logger based on rootdir, instr, service name and date"""

        # Create logger object
        name = f'koa_monitor_{instr}_{service}'
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        # paths
        processDir = f'{rootdir}/{instr.upper()}'
        logFile =  f'{processDir}/{name}.log'

        # create directory if it does not exist
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

        # add stdout to output so we don't need both log and print statements(>= warning only)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        # init message and return
        log.info(f'logger created for {instr} {service} at {logFile}')
        return log

    def handle_error(self, errcode, text='', check_time=True):
        '''Email admins the error but only if we haven't sent one recently.'''

        # always log/print
        self.log.error(f'{errcode}: {text}')
        handle_error(errcode, text, self.instr, self.service_name, check_time)


class KtlMonitor:
    '''
    Class to handle monitoring a distinct keyword for an instrument to
    determine when a new image has been written.

    Parameters:
        servicename (str): KTL service to monitor.
        keys (dict): Defines service and keyword to monitor
                     as well as special formatting to construct filepath.
        queue_mgr (obj): Class object that contains callback 'add_to_queue' function.
        log (obj): logger object
    '''
    def __init__(self, service_name, keys, queue_mgr, log):
        self.log = log
        self.service_name = service_name
        self.keys = keys
        self.queue_mgr = queue_mgr
        self.service = None
        self.last_mtime = None
        self.restart_count = 0
        self.resuscitations = None
        self.instr = keys['instr']
        self.log.info(f"KtlMonitor: instr: {self.instr}, service: "
                      f"{service_name}, trigger: {keys['trigger']}")

    def start(self):
        '''Start monitoring 'trigger' keyword for new files.'''

        keys = self.keys

        # get service instance
        try:
            self.service = ktl.Service(self.service_name)
        except Exception as e:
            self.log.error(traceback.format_exc())
            msg = (f"Could not start KTL monitoring for {self.instr} '{self.service}'. "
                   f"Retry in {KTL_START_RETRY_SEC} seconds.")
            self.queue_mgr.handle_error('KTL_START_ERROR', msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return

        # monitor keyword that indicates new file
        kw = self.service[keys['trigger']]
        kw.callback(self.on_new_file)

        # Prime callback to ensure it gets called at least once with current val
        if kw['monitored']:
            self.on_new_file(kw)
        else:
            kw.monitor()

        # establish heartbeat restart mechanism and service check interval
        # NOTE: Adding a couple seconds to heartbeat interval in case there
        # are edge cases to using exact heartbeat frequency
        hb = self.keys.get('heartbeat')
        if hb:
            period = hb[1] + 2
            self.service.heartbeat(hb[0], period)

            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()
            self.check_failed = False
            self.resuscitations = self.service.resuscitations

    def check_service(self):
        '''
        Periodically check that service is still working with a read of heartbeat keyword.
        Also keep tabs on resuscitation value and logs when it changes. This should indicate
        service reconnect.
        '''
        try:
            hb = self.keys['heartbeat'][0]
            kw = self.service[hb]
            kw.read(timeout=1)
            if self.service.resuscitations != self.resuscitations:
                self.log.debug(f"KTL service {self.service_name} resuscitations changed.")
            self.resuscitations = self.service.resuscitations
        except Exception as e:
            self.check_failed = True
            self.log.debug(f"{self.instr} KTL service '{self.service_name}' heartbeat read failed.")
            self.queue_mgr.handle_error('KTL_SERVICE_CHECK_FAIL', self.service_name)
        else:
            if self.check_failed:
                self.log.debug(f"KTL service {self.service_name} read successful after prior failure.")
            self.check_failed = False
        finally:
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()

    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''
        try:
            # Assume first read after a full restart is old
            if self.last_mtime is None:
                self.log.debug(f'Skipping (assuming first broadcast is old)')
                self.last_mtime = -1
                return

            # make sure keyword is populated
            if keyword['populated'] == False:
                self.log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return
            self.log.debug(f'on_new_file: {keyword.name}={keyword.ascii}')

            # Get trigger val and if 'reqval' is defined make sure trigger equals reqval
            keys = self.keys
            reqval = keys['val']
            if reqval is not None and reqval != keyword.ascii:
                self.log.info(f'Trigger val of {keyword.ascii} != {reqval}')
                return

            # get full file path
            format = self.keys.get('format', None)
            zfill = self.keys.get('zfill', None)
            if format:
                filepath = self.get_formatted_filepath(format, zfill)
            else:
                filepath = keyword.ascii

            # check for blank filepath
            if not filepath or not filepath.strip():
                self.log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

            # Some filepaths do not add the /s/ to the path which we need
            if filepath.startswith('/sdata'):
                filepath = f'/s{filepath}'

            # check for invalid filepath
            if '/sdata' not in filepath:
                self.log.error(f"INVALID FILEPATH (no 'sdata')\t{self.instr}\t{keyword.service}\t{filepath}")
                return
            if '/mira/' in filepath:
                self.log.error(f"INVALID FILE (mira)\t{self.instr}\t{keyword.service}\t{filepath}")
                return

            # Check file mod time and ensure it is not the same as last file (re-broadcasts)
            # (NOTE: preferred to checking last val read b/c observer can regenerate same filepath)
            try:
                mtime = os.stat(filepath).st_mtime
            except Exception as e:
                self.queue_mgr.handle_error('FILE_READ_ERROR', traceback.format_exc())
                return
            if self.last_mtime == mtime:
                self.log.debug(f'Skipping (last mtime = {self.last_mtime})')
                self.last_mtime = mtime
                return
            self.last_mtime = mtime

        except Exception as e:
            self.queue_mgr.handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        # send back to queue manager
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
            print(val)
            pad = zfill.get(key, None) if zfill else None
            if pad is not None:
                val = val.zfill(pad)
            filepath = filepath.replace('{'+key+'}', val)
        return filepath


def handle_error(errcode, text=None, instr=None, service=None, check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''

    # always log/print
    print(f'{errcode}: {text}')

    # Only send if we haven't sent one of same errcode recently
    if check_time:
        global last_email_times
        if not last_email_times: last_email_times = {}
        last_time = last_email_times.get(errcode)
        now = dt.datetime.now()
        if last_time and last_time + dt.timedelta(minutes=EMAIL_INTERVAL_MINUTES) > now:
            return
        last_email_times[errcode] = now

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail:
        return
    
    # Construct email message
    body = f'{errcode}\n{text}'
    subj = f'KOA MONITOR ERROR: [{instr} {service}] {errcode}'
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
