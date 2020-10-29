'''
Handle the various archiveing command line parameters and call DEP appropriately.
'''
import sys
import argparse
import configparser
import datetime as dt
import traceback
import os
import smtplib
from email.mime.text import MIMEText
import logging
import yaml
import db_conn
import importlib
from pathlib import Path
import logging

import dep
import instrument


#module globals
log = logging.getLogger('koa_dep')
last_email_times = None


def main():

#TODO: Add option to report query list only before running (ie --status ERROR --listonly)
    # Define inputs
    parser = argparse.ArgumentParser(description='DEP input parameters')
    parser.add_argument('instr', help='Keck Instrument')
    parser.add_argument('--filepath' , type=str, default=None, help='Filepath to FITS file to archive.')
    parser.add_argument('--dbid' , type=str, default=None, help='Database ID record to archive.')
    parser.add_argument('--tpx' , type=int, default=1, help='Update DB tables and transfer to IPAC.  Else, create files only and no transfer.')
    parser.add_argument('--reprocess', dest="reprocess", default=False, action="store_true", help='Replace DB record and files and rearchive')
    parser.add_argument('--starttime' , type=str, default=None, help='Start time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--endtime' , type=str, default=None, help='End time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--status' , type=str, default=None, help='Status to query for reprocessing.')
    parser.add_argument('--outdir' , type=str, default=None, help='Outdir match to query for reprocessing.')
    args = parser.parse_args()    

    #todo: if options require reprocessing or query, always prompt with confirmation 
    # and tell them how many files are affected and what the datetime range is

    #run it and catch any unhandled error for email to admin
    try:
        archive = Archive(args.instr, tpx=args.tpx, filepath=args.filepath, dbid=args.dbid, reprocess=args.reprocess,
                  starttime=args.starttime, endtime=args.endtime,
                  status=args.status, outdir=args.outdir)
    except Exception as error:
        email_error('ARCHIVE_ERROR', traceback.format_exc(), instr)


class Archive():

    def __init__(self, instr, tpx=1, filepath=None, dbid=None, reprocess=False, 
                 starttime=None, endtime=None, status=None, outdir=None):

        self.instr = instr
        self.tpx = tpx
        self.filepath = filepath
        self.dbid = dbid
        self.reprocess = reprocess
        self.starttime = starttime
        self.endtime = endtime
        self.status = status
        self.outdir = outdir

        #cd to script dir so relative paths work
        #todo: is this needed?
        os.chdir(sys.path[0])

        #load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        #create logger first
        #todo: not critical (try/except)
        global log
        log = self.create_logger('koa_dep', self.config[instr]['ROOTDIR'], instr)
        log.info("Starting DEP: " + ' '.join(sys.argv[0:]))

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

        #routing
        #todo: Add remaining options
        if filepath:
            self.process_file(instr, filepath, reprocess, tpx)
        elif dbid:
            self.process_id(instr, dbid, reprocess, tpx)
        elif starttime and endtime:
            self.reprocess_time_range(instr, starttime, endtime, tpx)
        elif status:
            self.reprocess_by_status(instr, status, tpx)
        elif outdir:
            self.reprocess_by_outdir(instr, outdir, tpx)
        else:
            log.error("Cannot run DEP.  Unable to decipher inputs.")

        log.info("DEP COMPLETE")


    def __del__(self):

        #Close the database connection
        if self.db:
            self.db.close()


    def create_logger(self, name, rootdir, instr):
        """Creates a logger based on rootdir, instr and date"""

        # Create logger object
        log = logging.getLogger(name)
        log.setLevel(logging.INFO)

        #paths 
        #NOTE: Using UTC so a night's data ends up in same log file.
        processDir = f'{rootdir}/{instr.upper()}'
        ymd = dt.datetime.utcnow().strftime('%Y%m%d')
        logFile =  f'{processDir}/{name}_{instr.upper()}_{ymd}.log'

        #create directory if it does not exist
        try:
            Path(processDir).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"ERROR: Unable to create logger at {logFile}.  Error: {str(e)}")
            return False

        # Create a file handler
        handle = logging.FileHandler(logFile)
        handle.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        #add stdout to output so we don't need both log and print statements(>= warning only)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        #init message and return
        log.info(f'logger created for {name} at {logFile}')
        print(f'Logging to {logFile}')
        return log


    def process_file(self, instr, filepath, reprocess, tpx, dbid=None):

        #TODO: Test that if an invalid instrument is used, this will throw an error and admin will be emailed.
        module = importlib.import_module('instr_' + instr.lower())
        instr_class = getattr(module, instr.capitalize())
        instr_obj = instr_class(instr, filepath, self.config, self.db, reprocess, tpx, dbid=dbid)

        ok = instr_obj.process()
        if not ok:
            self.handle_dep_error()
        else:
            log.info("DEP finished successfully!")


    def process_id(self, instr, dbid, reprocess, tpx):
        '''Archive a record by DB ID.'''

        self.process_file(instr, None, reprocess, tpx, dbid)


    def reprocess_time_range(self, instr, starttime, endtime, tpx):
        '''Look for fits files that have a UTC time within the range given and reprocess.'''

        #todo: this is pseudo code and untested
        #todo: Should we be limiting query by status too?
        starttime = starttime.replace('T', '')
        endtime = endtime.replace('T', '')
        query = (f"select * from dep_status where "
                 f"     utdatetime >= '{starttime}' "
                 f" and utdatetime <= '{endtime}' "
                 f" and instrument = '{instr}' ")
        rows = self.db.query('koa', query)

        for row in rows:
            self.process_file(instr, None, True, tpx, row['id'])


    def handle_dep_error(self):
        log.error("DEP ERROR!")
        #todo: Should we do anything here or leave it to dep.py to handle failed archiving?


def email_error(errcode, text, instr='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''
#todo: This won't work as intended b/c we are spawning single instances of archive.py

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
    subj = f'KOA DEP ERROR: [{instr}] {errcode}'
    msg = MIMEText(body)
    msg['Subject'] = subj
    msg['To']      = adminEmail
    msg['From']    = adminEmail
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


if __name__ == "__main__":
    main()
