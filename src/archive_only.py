#!/kroot/rel/default/bin/kpython3
"""
Desc: Daemon to poll koa_status for queued entries and archive them.
Handles only DB queue polling and archive processing. Does not monitor KTL.

Usage:
    python archive_only.py [service name]
    python archive_only.py kfcs
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


last_email_times = None
PROC_CHECK_SEC = 1.0
QUEUE_CHECK_SEC = 5.0
EMAIL_INTERVAL_MINUTES = 60


def main():
    """Handle command line args and create archive worker for service."""
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', help='The name of the instrument mode to process.')
    args = parser.parse_args()

    try:
        worker = ArchiveQueueWorker(args.mode)
    except Exception:
        handle_error('ARCHIVE_ONLY_ERROR', traceback.format_exc(), service=args.mode)
        sys.exit(1)

    while True:
        try:
            time.sleep(300)
            worker.log.info(f'Archive worker saying hi every 5 minutes ('
                            f'{worker.instr} {worker.service_uniquename})')
        except Exception as err:
            worker.log.error(f'Error waking up {err}.')
            break
    worker.log.info(f'Exiting {__file__}')


class ArchiveQueueWorker:
    """Poll koa_status for QUEUED entries and process them."""

    def __init__(self, inst_mode_name):
        self.procs = []
        self.max_procs = 10
        self.last_queue_check = None
        self.last_email_times = {}
        self.db = None

        os.chdir(sys.path[0])

        with open('config.live.ini') as f:
            self.config = yaml.safe_load(f)

        try:
            self.keys = monitor_config.instr_keymap[inst_mode_name]
            self.service_name = self.keys['ktl_service']
            try:
                self.service_uniquename = self.keys['ktl_uniquename']
            except Exception:
                self.service_uniquename = self.service_name
            self.instr = self.keys['instr']
        except KeyError:
            err = f"Instrument name: {inst_mode_name}, " \
                  f"{inst_mode_name}.ktl_service, and " \
                  f"{inst_mode_name}.instr must be defined in monitor_config.py"
            handle_error('CONFIG_ERROR', text=err)
            sys.exit(1)

        self.transfer = self.keys.get('transfer', False)

        self.utd = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')
        self.log = self.create_logger(self.config[self.instr]['ROOTDIR'],
                                      self.instr, self.service_uniquename)
        self.log.info(f"Starting KOA Archive Only for {self.instr} "
                      f"{self.service_name}")

        self._connect_db()
        self.worker_start()

    def _connect_db(self):
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE',
                                  persist=True, log_obj=self.log)

    def __del__(self):
        if self.db:
            self.db.close()

    def worker_start(self):
        self.process_monitor()
        self.queue_monitor()

    def process_monitor(self):
        """Remove any processes from list that are complete."""
        removed_procs = [proc for proc in self.procs if not proc.is_alive()]
        self.procs = [proc for proc in self.procs if proc.is_alive()]

        for proc in removed_procs:
            self.log.info(f'Removed completed process ID={proc.pid}, '
                          f'exitcode={proc.exitcode}')

        threading.Timer(PROC_CHECK_SEC, self.process_monitor).start()

    def check_queue(self, retry=True):
        """Check koa_status for queued jobs that need to be spawned."""
        self.last_queue_check = time.time()

        query = (f"select * from koa_status where level=0 "
                 f" and status='QUEUED' "
                 f" and instrument='{self.instr}' "
                 f" and service='{self.service_uniquename}' "
                 f" order by creation_time asc")                 # jph: removed 'limit 1' to allow multi rows per call

        rows = self._get_db_result('koa', query, get_one=False)  # jph: get_one=False to allow multi rows per call
                                                                 # jph: row -> rows, not row -> change all row to rows and handle multiple rows in loop
        if rows is False:
            self.log.debug(f'rows is False, query: {query}, row: {rows}')
            return False

        if len(rows) == 0:
            self.log.debug(f'rows == 0, query: {query}, row: {rows}')
            return False

        # if len(self.procs) >= self.max_procs:
        #     self.handle_error('MAX_PROCESSES', str(self.max_procs))
        #     return False

        available_slots = self.max_procs - len(self.procs)
        if available_slots <= 0:
            self.handle_error('MAX_PROCESSES', str(self.max_procs))
            return False

        rows_to_process = rows[:available_slots]
        if len(rows) > available_slots:
            self.log.info(f"Queue has {len(rows)} rows, processing {available_slots} this cycle due to max_procs={self.max_procs}.")

        for row in rows_to_process:
            # set status to PROCESSING
            update_query = f"update koa_status set status='PROCESSING' where id={row['id']}"
            result = self._get_db_result('koa', update_query)

            if result is False and retry:
                self.log.warning(f'DATABASE_ERROR,  retrying query: {update_query}')
                result = self._get_db_result('koa', update_query, retry=False)

            if result is False:
                self.handle_error('DATABASE_ERROR', update_query)
                continue

            # process row
            self.log.info(f"Processing DB record ID={row['id']}, "
                          f"filepath={row['ofname']}")

        query = f"update koa_status set status='PROCESSING' where id={row['id']}"
        result = self._get_db_result('koa', query)
        if result is False:
            if retry:
                self.log.warning(f'DATABASE_ERROR, retrying query: {query}')
                return self.check_queue(retry=False)
            self.handle_error('DATABASE_ERROR', query)
            return False

        self.log.info(f"Processing DB record ID={row['id']}, filepath={row['ofname']}")
        try:
            self.process_file(self.instr, row['id'])
        except Exception as err:
            self.handle_error('PROCESS_ERROR',
                              f"ID={row['id']}, filepath={row['ofname']}\n, {err}"
                              f"{traceback.format_exc()}")

    def queue_monitor(self):
        """Periodically check the queue when idle."""
        now = time.time()
        diff = int(now - self.last_queue_check) if self.last_queue_check else 0

        if diff >= QUEUE_CHECK_SEC or not self.last_queue_check:
            current_date = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')
            if self.utd != current_date:
                for handler in self.log.handlers[:]:
                    self.log.removeHandler(handler)

                self.utd = current_date
                self.log = self.create_logger(
                    self.config[self.instr]['ROOTDIR'],
                    self.instr, self.uniqueservice_name
                )

            self.check_queue()

        threading.Timer(QUEUE_CHECK_SEC, self.queue_monitor).start()

    def process_file(self, instr, dbid):
        """Spawn archiving for a single file by database ID."""
        proc = multiprocessing.Process(target=self.spawn_processing,
                                       args=(self.instr, dbid))
        proc.start()
        self.procs.append(proc)
        self.log.info(f'DEP started as system process ID: {proc.pid}')

    def spawn_processing(self, instr, dbid):
        """Call archiving for a single file by DB ID."""
        Archive(self.instr, dbid=dbid, transfer=self.transfer)

    def create_logger(self, rootdir, instr, service):
        """Create logger based on rootdir, instr, service name and date."""
        log_level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL,
        }
        log_level = log_level_map[self.config['MISC']['LOG_LEVEL']]

        name = f'rti_archive_{instr}_{service}'
        log = logging.getLogger(name)
        log.setLevel(log_level)

        process_dir = f'{rootdir}/log/{instr.upper()}'
        log_file = f'{process_dir}/{name}_{self.utd}.log'

        try:
            Path(process_dir).mkdir(parents=True, exist_ok=True)
            if not Path(log_file).is_file():
                with open(log_file, 'w') as file:
                    file.write('Log file created.')
        except Exception as err:
            raise RuntimeError(
                f"Unable to create logger at {log_file}. Error: {err}"
            ) from err

        handle = logging.FileHandler(log_file)
        handle.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        stdout_level = log_level_map[self.config['MISC']['STD_OUT_LOG_LEVEL']]
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(stdout_level)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)

        log.info(f'logger created for {instr} {service} at {log_file}')
        print(f'logger created for {instr} {service} at {log_file}')
        return log

    def handle_error(self, errcode, text='', check_time=True):
        self.log.error(f'{errcode}: {text}')
        handle_error(errcode, text, self.instr, self.service_uniquename, check_time)

    def _get_db_result(self, db_name, query, get_one=False, retry=True, filepath=None):
        self.log.debug(f'db params: {self.db}, {db_name}, {query}, '
                       f'{get_one}, {retry}, {filepath}')
        result = self.db.query(db_name, query, getOne=get_one)
        if result is False and retry:
            result = self._get_db_result(db_name, query, get_one=get_one,
                                         retry=False, filepath=filepath)
        return result


def handle_error(errcode, text=None, instr=None, service=None, check_time=True):
    """Email admins the error but only if we haven't sent one recently."""
    print(f'{errcode}: {text}')

    if check_time:
        global last_email_times
        if not last_email_times:
            last_email_times = {}
        last_time = last_email_times.get(errcode)
        now = dt.datetime.now()
        if last_time and last_time + dt.timedelta(minutes=EMAIL_INTERVAL_MINUTES) > now:
            return
        last_email_times[errcode] = now

    with open('config.live.ini') as f:
        config = yaml.safe_load(f)
    admin_email = config['REPORT']['ADMIN_EMAIL']
    if not admin_email:
        return

    body = f'{errcode}\n{text}'
    subj = f'KOA MONITOR ERROR: [{instr} {service}] {errcode}'
    msg = MIMEText(body)
    msg['Subject'] = subj
    msg['To'] = admin_email
    msg['From'] = admin_email
    smtp = smtplib.SMTP('localhost')
    smtp.send_message(msg)
    smtp.quit()


if __name__ == '__main__':
    main()
