#!/kroot/rel/default/bin/kpython3
"""
Desc: Daemon to monitor for new FITS files and add them to koa_status.
Handles only KTL monitoring and DB queue insertion. Does not process queued
entries.

Usage:
    python monitor_only.py [service name]
    python monitor_only.py kfcs
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
import ktl
import logging
import re
import hashlib
import glob

import monitor_config
import db_conn


last_email_times = None
KTL_START_RETRY_SEC = 60.0
SERVICE_CHECK_SEC = 60.0
EMAIL_INTERVAL_MINUTES = 60


def main():
    """Handle command line args and create monitor object for service."""
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', help='The name of the instrument mode to monitor.')
    args = parser.parse_args()

    try:
        monitor = Monitor(args.mode)
    except Exception:
        handle_error('MONITOR_ERROR', traceback.format_exc(), service=args.mode)
        sys.exit(1)

    while True:
        try:
            time.sleep(300)
            monitor.log.info(f'Monitor saying hi every 5 minutes ('
                             f'{monitor.instr} {monitor.service_uniquename})')
        except Exception as err:
            monitor.log.error(f'Error waking up {err}.')
            break
    monitor.log.info(f'Exiting {__file__}')


class Monitor:
    """Monitor KTL and insert new file rows into koa_status."""

    def __init__(self, inst_mode_name):
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

        self.utd = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')
        self.log = self.create_logger(self.config[self.instr]['ROOTDIR'],
                                      self.instr, self.service_uniquename)
        self.log.info(f"Starting KOA Monitor Only for {self.instr} "
                      f"{self.service_name}")

        self._connect_db()
        self.monitor_start()

    def _connect_db(self):
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE',
                                  persist=True, log_obj=self.log)

    def __del__(self):
        if self.db:
            self.db.close()

    def monitor_start(self):
        self.monitor = KtlMonitor(self.service_name, self.service_uniquename,
                                  self.keys, self, self.log)
        self.monitor.start()

    def add_to_queue(self, filepath, retry=True):
        """Add a file to koa_status as QUEUED."""
        try:
            if self.is_duplicate_file(filepath):
                return
        except Exception:
            self.log.error(traceback.format_exc())
            self.handle_error('DUPLICATE_FILE_CHECK_FAIL')

        self.log.info(f'Adding to queue: {filepath}')
        now = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        query = ("insert into koa_status set level=0,"
                 f"   instrument='{self.instr}' "
                 f" , service='{self.service_uniquename}' "
                 f" , ofname='{filepath}' "
                 f" , status='QUEUED' "
                 f" , creation_time='{now}' ")
        self.log.info(query)

        result = self._get_db_result('koa', query, filepath=filepath)
        if result is False:
            if retry:
                self.log.warning(f'DATABASE_ERROR, retrying query: {query}')
                return self.add_to_queue(filepath, retry=False)
            self.handle_error('DATABASE_ERROR', query)

    def is_duplicate_file(self, filepath, retry=True):
        """Check koa_status for a duplicate filepath/content match."""
        query = ("select * from koa_status "
                 f" where ofname='{filepath}' "
                 " order by id desc limit 1")

        row = self._get_db_result('koa', query, get_one=True)
        if row is False:
            if retry:
                self.log.warning(f'DATABASE_ERROR, retrying query: {query}')
                return self.is_duplicate_file(filepath, retry=False)
            return False

        if len(row) == 0:
            return False

        stage_file = row['stage_file']
        status = row['status']

        if not stage_file:
            if status in ('QUEUED', 'PROCESSING', 'TRANSFERRING', 'TRANSFERRED'):
                self.log.warning(f"Filepath '{filepath}' duplicate "
                                 f"broadcast same as {row['id']}. Skipping.")
                return True
            return False

        if not os.path.isfile(stage_file) or not os.path.isfile(filepath):
            return False

        md5_stage = self.get_file_md5(stage_file)
        md5_new = self.get_file_md5(filepath)
        if md5_stage == md5_new:
            self.log.warning(f"Filepath '{filepath}' is same hash as "
                             f"staged_file for DB ID {row['id']}. Skipping.")
            return True
        return False

    def get_file_md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

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

        name = f'rti_monitor_{instr}_{service}'
        log = logging.getLogger(name)
        log.setLevel(log_level)

        log_file = f'{rootdir}/log/{instr}/{name}_{self.utd}.log'

        try:
            Path(f'{rootdir}/log/{instr}').mkdir(parents=True, exist_ok=True)
            if not Path(log_file).is_file():
                with open(log_file, 'w') as file:
                    file.write('Log file created.')
        except Exception as err:
            print(f"ERROR: Unable to create logger at {log_file}.  Error: {str(err)}")
            return False

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
            if filepath is not None and self.is_duplicate_file(filepath):
                self.log.info(f'Database entry for {filepath} exists')
                return True
            result = self._get_db_result(db_name, query, get_one=get_one,
                                         retry=False, filepath=filepath)
        return result


class KtlMonitor:
    """Monitor a KTL keyword and hand files to the queue manager."""

    def __init__(self, service_name, service_uniquename, keys, queue_mgr, log):
        self.log = log
        self.service_name = service_name
        self.service_uniquename = service_uniquename
        self.keys = keys
        self.queue_mgr = queue_mgr
        self.service = None
        self.last_mtime = None
        self.restart_count = 0
        self.resuscitations = None
        self.instr = keys['instr']
        self.log.info(f"KtlMonitor: instr: {self.instr}, service: "
                      f"{service_name}, name: {service_uniquename}, "
                      f"trigger: {keys['trigger']}")
        self.delay = 0.25
        if 'delay' in self.keys:
            self.delay = self.keys['delay']

    def start(self):
        """Start monitoring trigger keyword for new files."""
        keys = self.keys

        try:
            self.service = ktl.Service(self.service_name)
        except Exception:
            self.log.error(traceback.format_exc())
            msg = (f"Could not start KTL monitoring for {self.instr} '{self.service}'. "
                   f"Retry in {KTL_START_RETRY_SEC} seconds.")
            self.queue_mgr.handle_error('KTL_START_ERROR', msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return

        kw = self.service[keys['trigger']]
        kw.callback(self.on_new_file)

        if kw['monitored']:
            self.on_new_file(kw)
        else:
            kw.monitor()

        hb = self.keys.get('heartbeat')
        if hb:
            period = hb[1] + 2
            self.service.heartbeat(hb[0], period)
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()
            self.check_failed = False
            self.resuscitations = self.service.resuscitations

    def check_service(self):
        """Check KTL heartbeat keyword periodically."""
        try:
            if self.service.resuscitations != self.resuscitations:
                self.log.info(f"KTL service {self.service_uniquename} resuscitations changed.")
            self.resuscitations = self.service.resuscitations
        except Exception as err:
            self.log.info('check_service() - heartbeat check failed')
            self.log.debug(err)
            self.check_failed = True
            self.log.info(f"{self.instr} KTL service '{self.service_uniquename}' heartbeat read failed.")
            self.queue_mgr.handle_error('KTL_SERVICE_CHECK_FAIL', self.service_uniquename)
        else:
            if self.check_failed:
                self.log.info(f"KTL service {self.service_uniquename} read successful after prior failure.")
            self.check_failed = False
        finally:
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()

    def on_new_file(self, keyword):
        """Callback for KTL monitoring. Gets full filepath and queues it."""
        try:
            if self.last_mtime is None:
                self.log.info('Skipping (assuming first broadcast is old)')
                self.last_mtime = -1
                return

            self.log.debug(f'last_mtime: {self.last_mtime}')

            if keyword['populated'] is False:
                self.log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            self.log.info(f'on_new_file: {keyword.name}={keyword.ascii}')

            keys = self.keys
            reqval = keys['val']
            if reqval is not None and reqval != keyword.ascii:
                self.log.info(f'Trigger val of {keyword.ascii} != {reqval}')
                return

            self.log.debug(f'keys, reqval: {keys} {reqval}')

            format_value = self.keys.get('format', None)
            zfill = self.keys.get('zfill', None)
            if format_value:
                filepath = self.get_formatted_filepath(format_value, zfill)
            else:
                filepath = keyword.ascii

            self.log.debug(f'filepath: {filepath}')

            if not filepath or not filepath.strip():
                self.log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

            if filepath.startswith('/sdata'):
                filepath = f'/s{filepath}'

            if not os.path.isfile(filepath):
                self.log.error(f"INVALID FILEPATH (file does not exist - {filepath}")
                return
            if not any(part in filepath for part in ('/sdata', '/operations', '/su-synoarchivedata')):
                self.log.error(f"INVALID FILEPATH (no 'sdata' or 'operations' or 'su-synoarchivedata')\t{self.instr}\t{keyword.service}\t{filepath}")
                return
            if '/osiris/test/' in filepath:
                self.log.error(f"INVALID FILEPATH\t{self.instr}\t{keyword.service}\t{filepath}")
                return
            if '/mira/' in filepath:
                self.log.error(f"INVALID FILE (mira)\t{self.instr}\t{keyword.service}\t{filepath}")
                return
            if '/hireseng/xdchange/' in filepath:
                self.log.error(f"INVALID FILE (hireseng/xdchange)\t{self.instr}\t{keyword.service}\t{filepath}")
                return
            if 'TEMPFITS.fits' in filepath:
                self.log.error(f"INVALID FILE {self.instr}\t{filepath}")
                return
            if re.search(r'KP.\d{8}.\d{5}.\d{2}-\d.fits', filepath):
                self.log.error(f"INVALID FILE {self.instr}\t{filepath}")
                return

            try:
                mtime = os.stat(filepath).st_mtime
            except FileNotFoundError:
                mtime = self._handle_file_not_found(filepath)
            except Exception as err:
                self.queue_mgr.handle_error(err, traceback.format_exc())

            if self.last_mtime == mtime:
                self.log.info(f'Skipping (last mtime = {self.last_mtime})')
                self.last_mtime = mtime
                return

            self.last_mtime = mtime
        except Exception:
            self.queue_mgr.handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        self.queue_mgr.add_to_queue(filepath)

    def _handle_file_not_found(self, filepath):
        """Wait briefly for file creation if filepath appeared early."""
        if self.instr == 'ESI' and self._chk_esi_test_file(filepath):
            return None

        for _ in range(0, 5):
            time.sleep(self.delay)
            try:
                return os.stat(filepath).st_mtime
            except Exception:
                self.log.info(f'delaying {self.delay}s, {filepath} not found')

        msg = f'FILE_READ_ERROR at {dt.datetime.now().strftime("%H:%M:%S")} '
        self.queue_mgr.handle_error(msg, traceback.format_exc())
        return None

    def _chk_esi_test_file(self, filepath):
        """Check to find if the broadcast is old and only a test file."""
        fits_dir = os.path.dirname(filepath)
        fits_files = glob.glob(f'{fits_dir}/*fits')
        try:
            chk_first = int(filepath.split('/')[-1].split('_')[-1].split('.')[0])
            first = chk_first == 1
        except Exception:
            first = False
        if not fits_files and not first:
            return True
        return False

    def get_formatted_filepath(self, format_value, zfill):
        """Construct filepath from multiple KTL keywords."""
        filepath = format_value
        matches = re.findall('{.*?}', format_value)
        for key in matches:
            key_name = key[1:-1]
            val = self.service[key_name].read()
            print(val)
            pad = zfill.get(key_name, None) if zfill else None
            if pad is not None:
                val = val.zfill(pad)
            filepath = filepath.replace('{' + key_name + '}', val)
        return filepath


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
