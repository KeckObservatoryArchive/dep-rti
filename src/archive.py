'''
Handle the various archiveing command line parameters and call DEP appropriately.
'''
import sys
import argparse
import datetime as dt
import traceback
import os
import smtplib
from email.mime.text import MIMEText
from common import create_logger, get_config
from db_conn import db_conn
import importlib
import glob


#module globals
last_email_times = None


def main():

    # Define inputs
    parser = argparse.ArgumentParser(description='DEP input parameters')
    parser.add_argument('instr', help='Keck Instrument')
    parser.add_argument('--filepath', type=str, default=None, help='Filepath to FITS file to archive.')
    parser.add_argument('--files', type=str, default=None, help='Directory path to FITS files.  Can use "glob" pattern match.')
    parser.add_argument('--dbid', type=str, default=None, help='Database ID record to archive.')
    parser.add_argument('--reprocess', dest="reprocess", default=False, action="store_true", help='Replace DB record and files and rearchive')
    parser.add_argument('--starttime', type=str, default=None, help='Start time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--endtime', type=str, default=None, help='End time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--status', type=str, default=None, help='Status to query for reprocessing.')
    parser.add_argument('--statuscode', type=str, default=None, help='Status code to (like) query for reprocessing.')
    parser.add_argument('--ofname', type=str, default=None, help='OFNAME match to query for reprocessing.')
    parser.add_argument('--progid', type=str, default=None, help='Override and assign PROGID to this value')
    parser.add_argument('--confirm', dest="confirm", default=False, action="store_true", help='Confirm query results.')
    parser.add_argument('--transfer', default=False, action='store_true', help='Transfer to IPAC and trigger IPAC API.  Else, create files only.')
    parser.add_argument('--level', type=int, default=0, help='Data reduction level. Only needed if reprocessing by query search.')
    args = parser.parse_args()    

    #run it 
    archive = Archive(args.instr, filepath=args.filepath, files=args.files, dbid=args.dbid, 
              reprocess=args.reprocess, starttime=args.starttime, endtime=args.endtime,
              status=args.status, statuscode=args.statuscode, ofname=args.ofname,
              progid=args.progid, confirm=args.confirm, transfer=args.transfer, level=args.level)


class Archive():

    def __init__(self, instr, filepath=None, files=None, dbid=None, reprocess=False, 
                 starttime=None, endtime=None, status=None, statuscode=None,
                 ofname=None, progid=None, confirm=False, transfer=False, level=0):

        #inputs
        self.instr = instr.upper()
        self.filepath = filepath
        self.files = files
        self.dbid = dbid
        self.reprocess = reprocess
        self.starttime = starttime
        self.endtime = endtime
        self.status = status
        self.statuscode = statuscode
        self.ofname = ofname
        self.progid = progid
        self.confirm = confirm
        self.transfer = transfer
        self.level = level
        self.logger = create_logger(f'koa.archive.{instr.lower()}')

        #other class vars
        self.db = None

        #handle any uncaught errors and email admin
        try:
            self.start()
        except Exception as err:
            email_error('ARCHIVE_ERROR', traceback.format_exc(), instr)


    def start(self):

        self.logger.info("STARTING PROCESSING")

        #cd to script dir so relative paths work
        os.chdir(sys.path[0])

        #load config file
        self.config = get_config()

        # Establish database connection 
        self.db = db_conn(persist=True)

        #routing
        if self.filepath:
            self.process_file(filepath=self.filepath)
        elif self.files:
            self.process_files(self.files)
        elif self.dbid:
            self.process_file(dbid=self.dbid)
        elif self.starttime or self.endtime or self.status or self.statuscode or self.ofname:
            self.reprocess_by_query()
        else:
            self.logger.error("ERROR: Unknown inputs.")

        self.logger.info("ALL PROCESSING COMPLETE")


    def __del__(self):

        #Close the database connection
        if self.db:
            self.db.close()


    def process_file(self, filepath=None, dbid=None):
        '''Creates instrument object by name and starts processing.'''
        module = importlib.import_module('instr_' + self.instr.lower())
        instr_class = getattr(module, self.instr.capitalize())
        logger_name = f'koa.{self.instr.lower()}' 
        instr_obj = instr_class(self.instr, filepath, self.reprocess,
                                self.transfer, self.progid, dbid=dbid, logger_name=logger_name)

        ok = instr_obj.process()
        if not ok:
            #NOTE: DEP has its own error reporting system so no need to do anything here.
            self.logger.warning("DEP finished with ERRORS!  See log file for details.")
        else:
            self.logger.info("DEP finished successfully.")


    def process_files(self, pattern):
        '''Search a directory for files to process.  Can use glob wildcard match.'''
        if pattern.endswith('/'): pattern += '*'

        files = []
        for filepath in glob.glob(pattern):
            files.append(filepath)

        if not self.confirm:
            print(f"\nSearching pattern: {pattern}\n")
            print("--------------------")
            for f in files: print(f)
            print("--------------------")
            self.logger.info(f"{len(files)} files found.  Use --confirm option to process these files.\n")
        else:
            for f in files:
                self.process_file(filepath=f)



    def reprocess_by_query(self):
        '''Query for fits files to reprocess.'''

        query = (f"select * from koa_status where level={self.level} and "
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
            self.logger.info(f"{len(rows)} records found.  Use --confirm option to process these records.\n")
        else:
            for row in rows:
                self.process_file(dbid=row['id'])


def email_error(errcode, text, instr='', check_time=True):
    '''Email admins the error but only if we haven't sent one recently.'''
    #NOTE: This won't work as intended if DEP called as single instance from monitor
    #but it is still useful for command line mode.

    #always print
    print(errcode, text)

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
    config = get_config()
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
