import argparse
import datetime as dt
import logging
from os import walk
from os.path import isdir, basename, dirname
import requests
import sys
import time
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler


def log_entry(msg):
    print(f'{dt.datetime.now()} {msg}')


class KpfDrp(FileSystemEventHandler):
    '''
    Handles the directory monitoring and processing of data.
    '''

    def __init__(self, instrument, level, datadir, rti, ops):

        self.running  = True
        self.level    = level
        self.rti      = rti

        self.instrument      = instrument
        self.datadir         = datadir

        self.rti_url = 'https://www3.keck.hawaii.edu/api/rti'

        self.params = {}
        self.params['instrument'] = self.instrument
        self.params['ingesttype'] = f'lev{self.level}'
        if not ops:
            self.params['testonly']   = 'true'
            self.params['dev']        = 'true'

        self.log = self.create_logger()
        self.log.info(f'Monitoring {self.datadir}')
        self.log.info(f'RTI API is {self.rti}')

        self.queue         = []
        self.fileList      = []

        self.add_current_file_list()

        self.running  = False

    def create_logger(self):
        '''
        Create a logger
        '''

        if self.level == 1:
            date_str = dt.datetime.utcnow().strftime('%Y%m%d')
        if self.level == 2:
            date_str = dt.datetime.now().strftime('%Y%m%d')

        name = f'kpfdrp_lev{str(self.level)}'
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        # Create a file handler
        log_file = f'/log/kpfdrp_lev{str(self.level)}_{date_str}.log'
        handle = logging.FileHandler(log_file)
        handle.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        # Init message and return
        log.info(f'logger created at {log_file}')

        return log

    def on_any_event(self, event):
        '''
        Callback to add new files to the queue.
        '''

        if event.is_directory:
            return

        # Skip if not a created event
        if event.event_type != 'created':
            return

        # Check the base filename
        filename = basename(event.src_path)
        if not filename.endswith('.fits') or filename in self.fileList:
            return

        self.log.info(f'{event.event_type} {event.src_path}')
        filename = event.src_path

        if filename in self.fileList:
            self.log.info(f'Skipping - already processed {filename}')
            return

        self.queue.append(filename)


    def add_current_file_list(self):
        '''
        Loops through and adds any files currently located in self.datadir
        to the queue.
        '''

        for root, dirs, files in walk(self.datadir):
            files.sort()
            for file in files:
                if not file.endswith('.fits'):
                    continue
                filename = f'{root}/{file}'
                self.queue.append(filename)

        self.log.info(f'Found {len(self.queue)} files in {self.datadir}')
        self.running = False


    def process_current_file_list(self):
        '''
        Processes all files in the queue, then empties the queue.
        '''

        self.running = True

        if len(self.queue) > 0:
            self.log.info(f'Processing queue with {len(self.queue)} entries')
        while len(self.queue) > 0:
            filename = self.queue[0]
            msg = 'sending to RTI' if self.rti else 'not sending to RTI'
            self.log.info(f'{filename} {msg}')
            if self.rti:
                self.params['datadir'] = dirname(filename)
                koaid                  = basename(filename)[0:20]
                self.params['koaid']   = f"{koaid}.fits"
                data = requests.get(self.rti_url,
                                    params=self.params, 
                                    auth=('koa','plate2'))

            self.fileList.append(filename)
            self.queue.remove(filename)

        self.running = False


def main():
    instrument = 'KPF'

    parser = argparse.ArgumentParser(description='KPF KOA DRP Watchdog')
    parser.add_argument('level', type=int, help='1 (L2 FITS only) or 2 (all)')
    parser.add_argument('--rti', default=False, action='store_true',
                        help='Notify RTI upon each successful reduction')
    parser.add_argument('--utdate', type=str, help='UT date to process',
                        default=dt.datetime.utcnow().strftime('%Y-%m-%d'))
    parser.add_argument('--ops', default=False, action='store_true', 
                        help='Operations mode: testonly = dev = false')
    args  = parser.parse_args()

    sleep_time = 30

    utdate = dt.datetime.strptime(args.utdate, '%Y-%m-%d')
    utdate_str = utdate.strftime('%Y%m%d')
    stopHour = 19 if args.level == 1 else 2
    log_entry(f'lev{args.level} archiving for {utdate_str} UT')

    # Watch the L2 directory during the night
    # Use the L1 directory during the day (all L2 have L1)
    level = 'L1' if args.level == 2 else 'L2'
    datadir = f'/kpfdata/data_drp/{level}/{utdate_str}'
    log_entry(f'Checking that directory exists {datadir}')

    # Wait for directory to exist and, if today, time check
    while not isdir(datadir):
        log_entry(f'Checking that directory exists {datadir}')
        time.sleep(sleep_time)
    log_entry(f'Directory exists {datadir}')

    # Setup monitoring of directory
    event_handler = KpfDrp(instrument,
                           level=args.level,
                           datadir=datadir,
                           rti=args.rti,
                           ops=args.ops)
    observer = PollingObserver(sleep_time)
    observer.schedule(event_handler, path=datadir, recursive=False)
    observer.start()

    try:
        # Stop lev1 at 9am (19 UT) and lev2 at 5pm (3 UT)
        while dt.datetime.utcnow().hour != stopHour:
            # Check the queue and process any files
            if not event_handler.running:
                event_handler.process_current_file_list()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        event_handler.log.info('User exit - goodbye')
    finally:
        observer.stop()
        event_handler.log.info(f'Watchdog stopped at {stopHour} UT - goodbye')

if __name__ == '__main__':
    main()
