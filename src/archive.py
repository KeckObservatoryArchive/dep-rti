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

    # Define inputs
    parser = argparse.ArgumentParser(description='DEP input parameters')
    parser.add_argument('instr', help='Keck Instrument')
    parser.add_argument('--filepath' , type=str, default=None, help='Filepath to FITS file to archive.')
    parser.add_argument('--dbid' , type=str, default=None, help='Database ID record to archive.')
    parser.add_argument('--reprocess', dest="reprocess", default=False, action="store_true", help='Replace DB record and files and rearchive')
    parser.add_argument('--starttime' , type=str, default=None, help='Start time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--endtime' , type=str, default=None, help='End time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--status' , type=str, default=None, help='Status to query for reprocessing.')
    parser.add_argument('--statuscode' , type=str, default=None, help='Status code to (like) query for reprocessing.')
    parser.add_argument('--ofname' , type=str, default=None, help='OFNAME match to query for reprocessing.')
    parser.add_argument('--confirm', dest="confirm", default=False, action="store_true", help='Confirm query results.')
    parser.add_argument('--transfer' , default=False, action='store_true', help='Transfer to IPAC and trigger IPAC API.  Else, create files only.')
    args = parser.parse_args()    

    #run it 
    archive = Archive(args.instr, filepath=args.filepath, dbid=args.dbid, reprocess=args.reprocess,
              starttime=args.starttime, endtime=args.endtime,
              status=args.status, statuscode=args.statuscode, ofname=args.ofname,
              confirm=args.confirm, transfer=args.transfer)


class Archive():

    def __init__(self, instr, filepath=None, dbid=None, reprocess=False, 
                 starttime=None, endtime=None, status=None, statuscode=None,
                 ofname=None, confirm=False, transfer=False):

        #inputs
        self.instr = instr.upper()
        self.filepath = filepath
        self.dbid = dbid
        self.reprocess = reprocess
        self.starttime = starttime
        self.endtime = endtime
        self.status = status
        self.statuscode = statuscode
        self.ofname = ofname
        self.confirm = confirm
        self.transfer = transfer

        #other class vars
        self.db = None

        #handle any uncaught errors and email admin
        try:
            self.start()
        except Exception as error:
            email_error('ARCHIVE_ERROR', traceback.format_exc(), instr)


    def start(self):

        #cd to script dir so relative paths work
        #todo: is this needed?  Does it work for both cmd line and monitor call?
        os.chdir(sys.path[0])

        #load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        #create logger first
        global log
        log = self.create_logger('koa_dep', self.config[self.instr]['ROOTDIR'], self.instr)
        log.info("Starting DEP")

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

        #routing
        if self.filepath:
            self.process_file(filepath=self.filepath)
        elif self.dbid:
            self.process_file(dbid=self.dbid)
        elif self.starttime or self.endtime or self.status or self.statuscode or self.ofname:
            self.reprocess_by_query()
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


    def process_file(self, filepath=None, dbid=None):
        '''Creates instrument object by name and starts processing.'''
        module = importlib.import_module('instr_' + self.instr.lower())
        instr_class = getattr(module, self.instr.capitalize())
        instr_obj = instr_class(self.instr, filepath, self.config, self.db, 
                                self.reprocess, self.transfer, dbid=dbid)

        ok = instr_obj.process()
        if not ok:
            self.handle_dep_error()
        else:
            log.info("DEP finished successfully!")


    def reprocess_by_query(self):
        '''Query for fits files to reprocess.'''

        query = (f"select * from dep_status where "
                 f" instrument = '{self.instr}' ")
        if self.status: 
            query += f" and status = '{self.status}' "
        if self.statuscode: 
            query += f" and status_code = '{self.statuscode}' "
        if self.ofname: 
            query += f" and ofname like '%{self.ofname}%' "
        if self.starttime: 
            starttime = self.starttime.replace('T', ' ')
            query += f" and utdatetime >= '{starttime}' "
        if self.endtime:   
            endtime = self.endtime.replace('T', ' ')
            query += f" and utdatetime <= '{endtime}' "
        query += " order by id asc"
        rows = self.db.query('koa', query)

        if not self.confirm:
            print(f"\n{query}\n")
            print("--------------------")
            for row in rows:
                print(f"{row['id']}\t{row['status']}\t{row['status_code']}\t{row['utdatetime']}\t{row['koaid']}\t{row['ofname']}")
            print("--------------------")
            print(f"{len(rows)} records found.  Use --confirm option to process these records.\n")
        else:
            for row in rows:
                self.process_file(dbid=row['id'])


    def handle_dep_error(self):
        #todo: call independent error reporting script which will query dep_status
        #and decide whether to email admins
        pass


def email_error(errcode, text, instr='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''
    #NOTE: This won't work as intended if DEP called as single instance from monitor
    #but it is still useful for command line mode.

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
