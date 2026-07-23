#!/kroot/rel/default/bin/kpython3
"""
Desc: Daemon to monitor for new FITS files and send to DEP for archiving.
Monitors KTL keywords to find new files for archiving.  Uses the database as its queue 
so the queue is not in memory.  Keeps a list of spawned processes to manage how many 
concurrent processes can run at once.  Run per instrument service.

Usage: 
    python monitor.py [service name]
    python monitor.py kfcs

Reference:
    http://spg.ucolick.org/KTLPython/index.html

"""
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
import logging

from archive import Archive
import monitor_config
import db_conn

# module globals
last_email_times = None
PROC_CHECK_SEC = 1.0
EMAIL_INTERVAL_MINUTES = 60
MAX_PROCS = 10

def main():
    """Handle command line args and create monitor object for service."""

    # Arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', help='The name of the instrument mode to monitor.')
    args = parser.parse_args()    

    # run monitors and catch any unhandled error for email to admin
    try:
        monitor = QueueMonitor(args.mode)
    except Exception as err:
        handle_error('QUEUE_MONITOR_ERROR: {err}', traceback.format_exc(), 
                     service=args.mode)
        sys.exit(1)

    # stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
            monitor.log.info(f'Queue monitor saying hi every 5 minutes ('
                              f'{monitor.instr} {monitor.service_uniquename})')
        except Exception as err:
            monitor.log.error(f'Error waking up {err}.')
            break
    monitor.log.info(f'Exiting {__file__}')


class QueueMonitor:
    """
    Monitors DB queue and spawns new DEP archive processes per datafile.
    """
    def __init__(self, inst_mode_name):

        # init other vars
        self.queue = []
        self.procs = []
        self.last_queue_check = None
        self.last_email_times = {}
        self.fdt_mode = 0
        self.db = None

        # cd to script dir so relative paths work
        os.chdir(sys.path[0])

        # load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        # get ktl-service-name and instrument from the name of instrument + mode
        try:
            self.keys = monitor_config.instr_keymap[inst_mode_name]
            self.service_name = self.keys['ktl_service']
            try:
                self.service_uniquename = self.keys['ktl_uniquename']
            except:
                self.service_uniquename = self.service_name
            try:
                self.queue_check_sec = self.keys['queue_check_sec']
            except:
                self.queue_check_sec = 10
            try:
                self.fdt_mode = self.keys['fdt_mode']
            except:
                self.fdt_mode = 0
            self.instr = self.keys['instr']
        except KeyError:
            err = f"Instrument name: {inst_mode_name}, " \
                  f"{inst_mode_name}.ktl_service, and " \
                  f"{inst_mode_name}.instr must be defined in monitor_config.py"
            handle_error('CONFIG_ERROR', text=err)
            sys.exit(1)

        self.transfer = self.keys.get('transfer', False)

        # create logger first
        self.utd = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')
        self.log = self.create_logger(self.config[self.instr]['ROOTDIR'], self.instr, self.service_uniquename)
        self.log.info(f"Starting RTI Queue Monitor for {self.instr} "
                      f"{self.service_uniquename}")
        if self.fdt_mode == 1:
            self.log.info("FDT mode is ON")
        
        # Establish database connection
        self._connect_db()

        self.monitor_start()

    def _connect_db(self):
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE',
                                  persist=True, log_obj=self.log)

    def __del__(self):

        # Close the database connection
        if self.db:
            self.db.close()

    def monitor_start(self):
        # start interval to monitor DEP processes for completion
        self.process_monitor()
        self.queue_monitor()

    def process_monitor(self):
        """Remove any processes from list that are complete."""

        # Loop procs and remove from list if complete
        # NOTE: looping in reverse so we can delete without messing up looping
        removed_procs = [p for p in self.procs if not p.is_alive()]
        self.procs = [p for p in self.procs if p.is_alive()]

        for p in removed_procs:
            self.log.info(f'Removed completed process ID={p.pid}, '
                          f'exitcode={p.exitcode}')

        # call this function every N seconds
        threading.Timer(PROC_CHECK_SEC, self.process_monitor).start()

    def check_queue(self, retry=True):
        """Check queue for jobs that need to be spawned."""
        self.last_queue_check = time.time()

        query = (f"select * from koa_status where level=0 "
                 f" and status='QUEUED' "
                 f" and instrument='{self.instr}' "
                 f" and service='{self.service_uniquename}' "
                 f" order by creation_time asc limit 5")

#        row = self._get_db_result('koa', query, get_one=True)
        row = self._get_db_result('koa', query)

        if row is False:
            self.log.debug(f'row is False, query: {query}, row: {row}')
            return False

        if len(row) == 0:
            self.log.debug(f'row == 0, query: {query}, row: {row}')
            return False 

        for r in row:
            # check that we have not exceeded max num procs
            if len(self.procs) >= MAX_PROCS:
                self.handle_error('MAX_PROCESSES', MAX_PROCS)
                return False

            # set status to PROCESSING
            query = f"update koa_status set status='PROCESSING' where id={r['id']}"

            result = self._get_db_result('koa', query)
            if result is False:
                if retry:
                    self.log.warning(f'DATABASE_ERROR,  retrying query: {query}')
                    return self.check_queue(retry=False)
                if not retry:
                    self.handle_error('DATABASE_ERROR', query)
                    return False

            # pop from queue and process it
            self.log.info(f"Processing DB record ID={r['id']}, "
                          f"filepath={r['ofname']}")
            try:
                self.process_file(self.instr, r['id'])
            except Exception as e:
                self.handle_error('PROCESS_ERROR',
                                  f"ID={r['id']}, filepath={r['ofname']}\n, {e}"
                                  f"{traceback.format_exc()}")

    def queue_monitor(self):
        """
        Periodically check the queue when idle.
        """
        now = time.time()
        diff = int(now - self.last_queue_check) if self.last_queue_check else 0

        if diff >= self.queue_check_sec or not self.last_queue_check:

#            # check if the ut date changed
#            current_date = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')
#            if self.utd != current_date:
#                # clear logs
#                for handler in self.log.handlers[:]:
#                    self.log.removeHandler(handler)
#
#                self.utd = current_date
#                self.log = self.create_logger(
#                    self.config[self.instr]['ROOTDIR'],
#                    self.instr,  self.service_name
#                )

            self.check_queue()

        # call this function every N seconds
        threading.Timer(self.queue_check_sec, self.queue_monitor).start()

    def process_file(self, instr, id):
        """
        Spawn archiving for a single file by database ID.

        # NOTE: Using multiprocessing instead of subprocess so we can spawn 
        # loaded functions as a separate process which saves us the ~0.5 second
        # overhead of launching python.
        """

        proc = multiprocessing.Process(target=self.spawn_processing,
                                       args=(self.instr, id))
        proc.start()
        self.procs.append(proc)
        self.log.info(f'DEP started as system process ID: {proc.pid}')

    def spawn_processing(self, instr, dbid):
        """Call archiving for a single file by DB ID."""
        obj = Archive(self.instr, 
                      dbid=dbid, 
                      transfer=self.transfer, 
                      fdt_mode=self.fdt_mode)

    def create_logger(self, rootdir, instr, service):
        """Creates a logger based on instr, service name and date"""
        log_level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        log_level = log_level_map[self.config['MISC']['LOG_LEVEL']]

        # Create logger object
        name = f'rti_queue_monitor_{instr}_{service}'
        log = logging.getLogger(name)

        log.setLevel(log_level)

        # paths
        logFile = f'{rootdir}/log/{instr}/{name}_{self.utd}.log'

        # create directory if it does not exist
        try:
            Path(f'{rootdir}/log/{instr}').mkdir(parents=True, exist_ok=True)

            # check that the file exists, if not create it.
            if not Path(logFile).is_file():
                with open(logFile, 'w') as file:
                    file.write('Log file created.')
        except Exception as e:
            print(f"ERROR: Unable to create logger at {logFile}.  Error: {str(e)}")
            return False

        # Create a file handler
        handle = logging.FileHandler(logFile)
        handle.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        # add stdout to output so we don't need both log and print statements
        # (>= warning only)
        log_level = log_level_map[self.config['MISC']['STD_OUT_LOG_LEVEL']]

        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        # init message and return
        log.info(f'logger created for {instr} {service} at {logFile}')

        # add to the std out log the location of the log
        print(f'logger created for {instr} {service} at {logFile}')

        return log

    def handle_error(self, errcode, text='', check_time=True):
        """Email admins the error but only if we haven't sent one recently."""

        # always log/print
        self.log.error(f'{errcode}: {text}')
        handle_error(errcode, text, self.instr, self.service_uniquename, check_time)

    def _get_db_result(self, db_name, query, get_one=False, retry=True, filepath=None):
        self.log.debug(f'db params: {self.db}, {db_name}, {query}, '
                       f'{get_one}, {retry}, {filepath}')
        result = self.db.query(db_name, query, getOne=get_one)
        if result is False and retry:
            if filepath != None:
                if self.is_duplicate_file(filepath):
                    self.log.info(f'Database entry for {filepath} exists')
                    return True

            result = self._get_db_result(db_name, query, get_one=get_one,
                                         retry=False, filepath=filepath)

        return result

def handle_error(errcode, text=None, instr=None, service=None, check_time=True):
    """Email admins the error but only if we haven't sent one recently."""

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
    subj = f'KOA QUEUE MONITOR ERROR: [{instr} {service}] {errcode}'
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
