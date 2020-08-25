'''
Class to handle the various command line parameters and call DEP appropriately.
'''
import sys
import argparse
from datetime import datetime as dt
import configparser
import traceback
import os
import smtplib
from email.mime.text import MIMEText
import logging
import yaml
import db_conn
import importlib

import dep
import instrument


class Archive():

    def __init__(instr, filepath=None, reprocess=False, starttime=None, endtime=None, status=None, outdir=None):

        #cd to script dir so relative paths work
        #todo: is this needed?
        os.chdir(sys.path[0])

        #load config file
        with open('config.live.ini') as f: 
            self.config = yaml.safe_load(f)

        #create logger first
        self.create_logger('koadep', self.config['ROOTDIR'], instr)
        log = logging.getLogger('koadep')
        log.info("Starting DEP")

        # Establish database connection 
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

        #routing
        if filepath:
            process_file(instr, filepath, reprocess)
        elif starttime and endtime:
            reprocess_time_range(instr, starttime, endtime)
        elif status:
            reprocess_by_status(instr, status)
        elif outdir:
            reprocess_by_outdir(instr, outdir)
        else:
            log.error("Cannot run DEP.  Unable to decipher inputs.")


    def __del__(self):
        """Destructor"""

        #Close the database connection
        if self.db:
            self.db.close()


    def create_log(self, name, rootdir, instr):
        """Creates a logger based on rootdir, instr and date"""

        # Create logger object
        log = lg.getLogger(name)
        log.setLevel(lg.INFO)

        # Create a file handler
        processDir = f'{rootdir}/{instr.upper()}'
        ymd = datetime.utcnow().strftime('%Y%m%d')
        logFile =  f'{processDir}/dep_{instr.upper()}_{ymd}.log'
        handle = lg.FileHandler(logFile)
        handle.setLevel(lg.INFO)
        formatter = lg.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        handle.setFormatter(formatter)
        log.addHandler(handle)

        #add stdout to output so we don't need both log and print statements(>= warning only)
        sh = lg.StreamHandler(sys.stdout)
        sh.setLevel(lg.WARNING)
        formatter = lg.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        log.addHandler(sh)
        
        #init message and return
        log.info('logger created')


    def reprocess_time_range(instr, starttime, endtime):
        '''Look for fits files that have a UTC time within the range given and reprocess.'''

        #todo: this is pseudo code and untested
        #todo: We may store header info in dep_status.header_json
        starttime = starttime.replace('T', '')
        endtime = endtime.replace('T', '')
        query = (f"select * from dep_status where ",
                 f"     datetime >= '{starttime}' ",
                 f" and datetime <= '{endtime}' ")
        files = self.db.query(query)

        for f in files:
            self.process_file(instr, f['savepath'], True)


    def process_file(instr, filepath, reprocess):

        instr_obj = create_instr_obj(instr, self.filepath, self.config, self.db, self.reprocess)
        ok = instr_obj.process()
        if not ok:
            handle_dep_error()
        else:
            log.info("DEP finished successfully!")


    def create_instr_obj(instr):
        #TODO: Test if an invalid instrument is used, this will throw an error and admin will be emailed.
        module = importlib.import_module('instr_' + instr.lower())
        instr_class = getattr(module, instr.capitalize())
        instr_obj = instr_class(instr)
        return instr_obj


def handle_fatal_error(args):

    #form subject and msg (and log as well)
    subject = f'DEP ERROR: {args}'
    msg = traceback.format_exc()
    log.error(subject + ' ' + msg)

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    msg = MIMEText(msg)
    msg['Subject'] = subject
    msg['To']      = adminEmail
    msg['From']    = adminEmail
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


def create_logger(name, logdir):
    try:
        #Create logger object
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        #file handler (full debug logging)
        logfile = f'{logdir}/{name}.log'
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.DEBUG)
        handler.suffix = "%Y%m%d"
        logger.addHandler(handler)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        #stream/console handler (info+ only)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(' %(levelname)8s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    except Exception as error:
        print (f"ERROR: Unable to create logger '{name}' in dir {logfile}.\nReason: {str(error)}")


if __name__ == "__main__":

    # Define Input parameters
    parser = argparse.ArgumentParser(description='DEP input parameters')
    parser.add_argument('instr', help='Keck Instrument')
    parser.add_argument('--filepath' , type=str, default=None, help='Filepath to FITS file for archiving.')
    parser.add_argument('--reprocess', dest="reprocess", default=False, action="store_true", help='Replace DB record and files and rearchive')
    parser.add_argument('--starttime' , type=str, default=None, help='Start time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--endtime' , type=str, default=None, help='End time to query for reprocessing. Format yyyy-mm-ddTHH:ii:ss.dd')
    parser.add_argument('--status' , type=str, default=None, help='Status to query for reprocessing.')
    parser.add_argument('--outdir' , type=str, default=None, help='Outdir match to query for reprocessing.')
    parser.add_argument('--koaxfr', dest="dev", default=False, action="store_true", help='')
    args = parser.parse_args()    

    #todo: if options require reprocessing or query, always prompt with confirmation 
    # and tell them how many files are affected and what the datetime range is

    try:
        archive = Archive(instr, filepath=args.filepath, reprocess=args.reprocess,
                  starttime=args.starttime, endtime=args.endtime,
                  status=args.status, outdir=args.outdir)
    except Exception as error:
        handle_fatal_error(sys.argv)
