#-------------------------------------------------------------------------------
# dep.py instr utDate [tpx]
#
# This is the backbone process for KOA operations at WMKO.
#
# instr = Instrument name (e.g. HIRES)
# utDate = UT date of observation (YYYY-MM-DD)
#
#-------------------------------------------------------------------------------

import os
import sys
import importlib
from dep_obtain import dep_obtain
from dep_locate import dep_locate
from dep_add import dep_add
from dep_dqa import dep_dqa
from dep_drp import dep_drp
from dep_tar import dep_tar
from koaxfr import koaxfr
from send_email import send_email
from common import *
import re
import datetime as dt
from dateutil import parser
import db_conn
import yaml


class Dep:
    """
    This is the backbone class for KOA operations at WMKO.

    @param instr: instrument name
    @type instr: string
    @param utDate: UT date of observation
    @type utDate: string (YYYY-MM-DD)
    """
    def __init__(self, instr, utDate, tpx=0, configArgs=[]):
        """
        Setup initial parameters.
        Create instrument object.
        """
        self.instr = instr.upper()
        self.utDate = utDate
        self.tpx = tpx
        if self.tpx != 1: self.tpx = 0

        #parse config file and add in command line args
        with open('config.live.ini') as f: self.config = yaml.safe_load(f)
        for c in configArgs:
            section = c['section']
            key     = c['key']
            val     = c['val']
            self.config[section][key] = val

        # Create instrument object
        className = self.instr.capitalize()
        module = importlib.import_module('instr_' + self.instr.lower())
        instrClass = getattr(module, className)
        self.instrObj = instrClass(self.instr, self.utDate, self.config)
        
        # Open database connection if in config
        self.db = db_conn.db_conn('config.live.ini', configKey='DATABASE', persist=True)

       
    def __del__(self):
        """
        Close the database connection, if it is open
        """
        if self.db:
            self.db.close()

 
    def go(self, processStart=None, processStop=None):
        """
        Processing steps for DEP
        @param processStart: name of process to start at.  Default is 'obtain'
        @type instr: string
        @param processStop: name of process to stop after.  Default is 'koaxfr'
        @type instr: string
        """

        #process steps control (pair down ordered list if requested)        
        steps = ['obtain', 'locate', 'add', 'dqa', 'lev1', 'tar', 'koaxfr']
        if (processStart == None): processStart = steps[0]
        elif (processStart != None and processStart not in steps):
            raise Exception('Incorrect use of processStart: ' + processStart)
            return False
        if (processStop == None): processStop = steps[-1]
        elif (processStop != None and processStop not in steps):
            raise Exception('Incorrect use of processStop: ' + processStop)
            return False
        if processStart != None: steps = steps[steps.index(processStart):]
        if processStop  != None: steps = steps[:(steps.index(processStop)+1)]
        print (f'DEP: process start: {processStart}, process stop: {processStop}')

        #check if full run.  Prompt if not full run and doing tpx updates
        fullRun = True if (processStart == 'obtain' and processStop == 'koaxfr') else False
        reprocess = int(self.config.get('MISC', {}).get('REPROCESS', 0))
        if (fullRun == False and self.tpx and not reprocess): 
            self.prompt_confirm_tpx()


        # Init DEP process (verify inputs, create the logger and create directories)
        # NOTE: A full run assert fails if dirs exist, otherwise assumes you know what you are doing.
        self.instrObj.dep_init(fullRun)


        #check koa for existing entry
        if fullRun and self.tpx:
            if not self.check_koa_db_entry(): return False


        #check 24 time window vs runtime
        if fullRun:
            if not self.check_runtime_vs_window(): return False


        #write to tpx at dep start
        if fullRun and self.tpx:
            utcTimestamp = dt.datetime.utcnow().strftime("%Y%m%d %H:%M")
            update_koatpx(self.instrObj.instr, self.instrObj.utDate, 'start_time', utcTimestamp, self.instrObj.log)


        #run each step in order
        for step in steps:
            self.instrObj.log.info('*** RUNNING DEP PROCESS STEP: ' + step + ' ***')

            if   step == 'obtain': dep_obtain(self.instrObj)
            elif step == 'locate': dep_locate(self.instrObj, self.tpx)
            elif step == 'add'   : dep_add(self.instrObj)
            elif step == 'dqa'   : dep_dqa(self.instrObj, self.tpx)
            elif step == 'lev1'  : dep_drp(self.instrObj, step, self.tpx)
            elif step == 'tar'   : dep_tar(self.instrObj, self.tpx)
            elif step == 'koaxfr': koaxfr(self.instrObj, self.tpx)

            #check for expected output
            self.check_step_results(step)


        #special metadata compare report for reprocessing?
        meta_compare_dir = self.config.get('MISC', {}).get('META_COMPARE_DIR')
        if meta_compare_dir: self.do_meta_compare(meta_compare_dir)


        #email completion report
        admin_email = self.config.get('REPORT', {}).get('ADMIN_EMAIL')
        email_report = int(self.config.get('MISC', {}).get('EMAIL_REPORT', 0))
        if admin_email and (fullRun or email_report == 1): 
            self.do_process_report_email(admin_email)


        #complete
        self.instrObj.log.info('*** DEP PROCESSSING COMPLETE! ***')
        print ('*** DEP PROCESSSING COMPLETE! ***')
        return True


    def check_koa_db_entry(self):
        """
        Verify whether or not processing can proceed.  Processing cannot
        proceed if there is already an entry in koa.koatpx.
        """

        self.instrObj.log.info('dep: verifying if can proceed')
        query = f'select utdate as num from koatpx where instr="{self.instr}" and utdate="{self.utDate}"'
        data = self.db.query('koa', query)
#todo: test this
        if data is False:
            raise Exception('dep: could not query koa database. EXITING!')
            return False
        elif data and len(data) > 0:
            raise Exception('dep: entry already exists in database. EXITING!')
            return False
        else:
            return True


    def check_runtime_vs_window(self):
        '''
        Verify that we are not starting a run within the 24 hour defined time window
        NOTE: If we are after, that is ok.  If we are before, we are probably doing some cleanup work. 
        '''

        endTimeStr = self.instrObj.utDate + ' ' + self.instrObj.endTime
        endTime = dt.datetime.strptime(endTimeStr, "%Y-%m-%d %H:%M:%S")
        yesterTime = endTime - dt.timedelta(days=1)

        curTime = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%s %H:%M:%S')
        curTime = dt.datetime.utcnow()

        #todo: error message but returning true for now.  Should we stop processing or just warn with big error?
        if curTime > yesterTime and curTime < endTime:
            self.instrObj.log.error('dep: Runtime is within 24 hour search window!')
            return True

        return True


    def prompt_confirm_tpx(self):

        sys.stdout.write("\n===> ATTENTION: You are doing a manual run with TPX updates ON!  Enter [y]es to continue: ")
        choice = input().lower()

        allowed = {'yes', 'ye', 'y'}
        if choice not in allowed:
            sys.stdout.write("EXITING!\n")
            sys.stdout.write("EXITING!\n")
            sys.stdout.write("EXITING!\n")
            sys.exit()



    def check_step_results(self, step):

        self.instrObj.log.info('*** VERIFYING OUTPUT FOR : ' + step + ' ***')

        #useful vars
        dirs = self.instrObj.dirs
        instr = self.instrObj.instr
        utDate = self.instrObj.utDate
        utDateDir = self.instrObj.utDateDir


        #get list of files to check for existence
        checkFiles = []
        if   step == 'obtain':
            checkFiles.append(dirs['stage'] + '/dep_obtain' + instr + '.txt')
        elif step == 'locate':
            checkFiles.append(dirs['stage'] + '/dep_locate' + instr + '.txt')
        elif step == 'add':
            #note: dep_add should not exit if weather files are not found
            pass
        elif step == 'dqa':
            checkFiles.append(dirs['stage'] + '/dep_dqa' + instr + '.txt')
            if os.path.isfile(checkFiles[0]):
                with open(checkFiles[0], 'r') as f:
                    count = sum(1 for line in f)
                if count > 0:
                    checkFiles.append(dirs['lev0'] + '/' + utDateDir + '.filelist.table')
                    checkFiles.append(dirs['lev0'] + '/' + utDateDir + '.metadata.table')
                    checkFiles.append(dirs['lev0'] + '/' + utDateDir + '.metadata.md5sum')
                    checkFiles.append(dirs['lev0'] + '/' + utDateDir + '.FITS.md5sum.table')
                    checkFiles.append(dirs['lev0'] + '/' + utDateDir + '.JPEG.md5sum.table')
        elif step == 'tar':
            checkFiles.append(dirs['anc'] + '/anc' + utDateDir + '.tar.gz')
            checkFiles.append(dirs['anc'] + '/anc' + utDateDir + '.md5sum')
        elif step == 'lev1':
            #todo: Anything to do here?
            pass


        #check for file existence and fatal error if not found
        for file in checkFiles:
            if not os.path.exists(file):
                self.do_fatal_error(step, 'Process post-check: ' + file + " not found!")
                break


    def do_meta_compare(self, meta_compare_dir):

        dirs = self.instrObj.dirs
        utDateDir = self.instrObj.utDateDir

        files = []
        files.append(meta_compare_dir + '/lev0/' + utDateDir + '.metadata.table')
        files.append(dirs['lev0'] + '/' + utDateDir + '.metadata.table')

        import metadata
        results = metadata.compare_meta_files(files , skipColCompareWarn=True)
        if not results:
            self.instrObj.log.error("Could not compare files: " + str(files))
            return
        else:
            for result in results:
                self.instrObj.log.info(result['compare'])
                for warn in result['warnings']:
                    self.instrObj.log.warning(warn)


    def do_process_report_email(self, admin_email):

        #read log file for errors and warnings
        logStr = ''
        progStr = ''
        count = 0
        errCount = 0
        warnCount = 0
        logOutFile = self.instrObj.log.handlers[0].baseFilename
        with open(logOutFile, 'r') as log:
            for line in log:
                count += 1
                if re.search('WARNING', line, re.IGNORECASE):
                    pos = line.upper().find('WARNING')
                    logStr += str(count) + ': ' + line[pos:].strip() + "\n"
                    warnCount += 1
                elif re.search('ERROR', line, re.IGNORECASE):
                    pos = line.upper().find('ERROR')
                    logStr += str(count) + ': ' + line[pos:].strip() + "\n"
                    errCount += 1
                elif re.search('PROGID COUNT', line, re.IGNORECASE):
                    pos = line.find('PROGID COUNT') + 14
                    progStr += line[pos:].strip() + "\n"


        #form subject
        subject = ''
        if (errCount  > 0): subject += '(ERR:'  + str(errCount) + ')'
        if (warnCount > 0): subject += '(WARN:' + str(warnCount) + ')'
        subject += ' DEP : ' + self.instrObj.instr + ' ' + self.instrObj.utDate


        #form msg
        msg = ""

        msg += "===== PROGRAM ASSIGNMENT COUNTS ====\n"
        msg += progStr + "\n"

        msg += "===== ERRORS AND WARNINGS ====\n"
        if (logStr == ''): msg += "  (none)\n"
        else:              msg += logStr + "\n"

        msg += "\n===== KOAID + RAW FILE LIST =====\n"
        dirs = self.instrObj.dirs
        utDateDir = self.instrObj.utDateDir
        filelistOutFile = dirs['lev0'] + '/' + utDateDir + '.filelist.table'
        if os.path.isfile(filelistOutFile): 
            with open(filelistOutFile, 'r') as file:
                for line in file: 
                    msg += line.strip() + "\n"
        else: msg += "  0 Total FITS files\n"

        
        #if admin email then email
        if admin_email:
            send_email(admin_email, admin_email, subject, msg)



    def do_fatal_error(self, step, msg):

        #call common.do_fatal_error
        do_fatal_error(msg, self.instrObj.instr, self.instrObj.utDate, step, self.instrObj.log)


        #update tpx
        if self.tpx:
            self.instrObj.log.info('Updating KOA database with error status.')
            utcTimestamp = dt.datetime.utcnow().strftime("%Y%m%d %H:%M")
            update_koatpx(instr, utDate, 'arch_stat', "ERROR", log)
            update_koatpx(instr, utDate, 'arch_time', utcTimestamp, log)

        #exit program
        self.instrObj.log.info('EXITING DEP!')
        sys.exit()


#------- End dep class --------
