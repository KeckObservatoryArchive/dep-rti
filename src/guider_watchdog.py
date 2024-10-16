#! /kroot/rel/default/bin/kpython3

import argparse
import datetime as dt
import ktl
import logging
import sys
import time
import watchdog.events
from getpass import getuser
from os import walk, makedirs, chdir, remove, system
from os.path import isdir, isfile, islink, basename, dirname
from socket import gethostname
from watchdog.events import PatternMatchingEventHandler
#from watchdog.events import RegexMatchingEventHandler
from watchdog.observers.polling import PollingObserver
from astropy.io import fits

class KoaGuiderWatchdog(PatternMatchingEventHandler):
    '''
    Handles the directory monitoring and processing
    '''
    def __init__(self, telnr, datadir, destdir):
        self.running  = True
        self.telnr = telnr
        self.datadir = datadir
        self.destdir = destdir

        self.log = self.create_logger()
        self.log.info(f'Monitoring {self.datadir}')

        PatternMatchingEventHandler.__init__(self,
                                             patterns=['*.fits'],
                                             ignore_directories=True,
                                             case_sensitive=False)


    def create_logger(self):
        """Creates a logger"""

        # Create logger object
        name = f'guider_watchdog_k'
        name += '1' if self.telnr == 1 else '2'
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)

        # paths                   
        logFile =  f'/log/{name}.log'

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
        log.info(f'logger created at {logFile}')
        return log


    def on_created(self, event):
        self.log.info("Watchdog received created event - % s." % event.src_path)
        self.log.info("src_path: % s" % event.src_path)

        # read fits header
        has_camname = False
        instr_name = 'UNDEFINED'
        result = 'UNDEFINED'
        hdul = None

        # consider breaking fits.open() into a separate try block
        # to deal with when  fits.open() hangs
        try:
            hdul = fits.open(event.src_path)
            #hdul_len = len(hdul[0].header)
            instr_name = hdul[0].header['currinst']
            result = hdul[0].header['camname']
            if result:
                has_camname = True
            else:
                has_camname = False
        except:
            has_camname = False
        finally:
            if has_camname == True:
                if self.telnr == 1:
                    ktl_keyword = 'koa.k1guiderfile'
                    ktl_keyword_name = 'K1GUIDERFILE'
                else:
                    ktl_keyword = 'koa.k2guiderfile'
                    ktl_keyword_name = 'K2GUIDERFILE'
               
                if 'pcs' not in result and 'ssc' not in result and not islink(event.src_path):
                    keyword = ktl.cache(ktl_keyword)
                    keyword.write(event.src_path)
                    value = keyword.read()
                    self.log.info("Service=koa, Keyword=" + ktl_keyword_name + ", Value=" + value)
                else:
                    self.log.info("Ignored CAMNAME=PCS|SSC or symlink: " + event.src_path)
            else:
                self.log.info("Ignored: No CAMNAME " + event.src_path + " for " + instr_name)
            hdul.close()

def main():

    parser = argparse.ArgumentParser(description='Guider Quicklook DRP')
    parser.add_argument('--telnr', type=int, default=1, 
                        help='Specify telescope number: 1=k1 (default) or 2=k2')
    parser.add_argument('--datadir', type=str, help='Source data dir to process',
                        default=".")
    parser.add_argument('--destdir', type=str, help='Destination data dir to process',
                        default=".")
    parser.add_argument('--utdate', type=str, help='UT date to process',
                        default=dt.datetime.utcnow().strftime('%Y-%m-%d'))
    parser.add_argument('--manual', dest='manual', default=False,
                        action='store_true', help='Manual run, disable end hour')

    args = parser.parse_args()
    telnr = args.telnr
    datadir = args.datadir
    destdir = args.destdir
    manual = args.manual

    utdate = dt.datetime.strptime(args.utdate, '%Y-%m-%d')
    tonight = (utdate - dt.timedelta(days=1)).strftime('%Y-%m-%d')

    # Setup logging
    logging.basicConfig(level=logging.INFO,
                        format = '%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Setup monitoring of directory
    event_handler = KoaGuiderWatchdog(telnr, datadir, destdir)
    observer = PollingObserver(30)
    observer.schedule(event_handler, path=datadir, recursive=True)
    observer.start()

    event_handler.log.info("TELNR: " + str(telnr))
    event_handler.log.info("DATADIR: " + datadir)
    event_handler.log.info("DESTDIR: " + destdir)
    event_handler.log.info("UTDATE: " + str(utdate))
    event_handler.log.info("TONIGHT: " + str(tonight))
    event_handler.log.info("MANUAL: " + str(manual))

    # ops
    beginHour = 4 if not manual else 0  # UTC
    endHour = 19 if not manual else 24  # UTC
    wait_time = 30

    try:
        while True:
            # Stop the DRP if 9am or later
            hourNow = int(dt.datetime.utcnow().strftime('%H'))
            
            if hourNow >= beginHour and hourNow < endHour:
                if observer.is_alive():
                    time.sleep(wait_time)
            else: 
                if observer.is_alive():
                    event_handler.log.info("Sky Hours (19:00-9:00 HST or 4:00-19:00 UTC)...")
                    event_handler.log.info("...Stopping Watchdog Observer at hour " + str(hourNow) + " UTC...")
                    observer.stop()
                event_handler.log.info("...Exiting Guider Watchdog...")
                sys.exit()

    except KeyboardInterrupt:
        event_handler.log.info("Terminating watchdog due to interrupt...")
        observer.stop()

    event_handler.log.info("...Joining (Thread Blocker) Watchdog Observer - Please Wait...")
    observer.join()

if __name__ == '__main__':
    main()
