'''
Data Evaluation and Processing
'''

import os
import sys
import importlib
import urllib.request
import json
import numpy as np
import re
import math
import db_conn
import yaml
from astropy.io import fits
import datetime as dt
import shutil
import glob
import inspect
import fnmatch
import pathlib

import metadata
import update_koapi_send
from common import *
from envlog import *

import logging
log = logging.getLogger('koa_dep')


class DEP:

    def __init__(self, instr, filepath, config, db, reprocess, transfer, dbid=None):

        #class inputs
        self.instr     = instr.upper()
        self.filepath  = filepath
        self.config    = config
        self.db        = db
        self.reprocess = reprocess
        self.transfer  = transfer
        self.dbid      = dbid

        #init other vars
        self.koaid = ''
        self.fits_hdu = None
        self.fits_hdr = None
        self.extra_meta = {}
        self.errors = []
        self.stage_file = None
        self.ofname = None
        self.utdatedir = None
        self.utc = None
        self.utdate = None
        self.dirs = None

        #other helpful vars
        self.rootdir = self.config[self.instr]['ROOTDIR']
        if self.rootdir.endswith('/'): self.rootdir = self.rootdir[:-1]


    #abstract methods that must be implemented by inheriting classes
    def run_dqa(self) : raise NotImplementedError("Abstract method not implemented!")


    def process(self):
        '''Run all prcessing steps required for archiving end to end'''

        ok = True
        if ok: ok = self.check_status_db_entry()
        if ok: ok = self.load_fits()
        if ok: ok = self.set_koaid()
        if ok: ok = self.processing_init()
        if ok: ok = self.check_koaid_db_entry()
        if ok: ok = self.validate_fits()
        if ok: ok = self.run_psfr()
        if ok: ok = self.run_dqa()
        if ok: ok = self.write_lev0_fits_file() 
        if ok:      self.make_jpg()
        if ok: ok = self.create_meta()
        if ok:      self.create_ext_meta()
        if ok: ok = self.run_drp()
        if ok:      self.check_koapi_send()
        if ok: ok = self.create_readme()
        if ok: ok = self.update_dep_stats()
        if ok: ok = self.transfer_ipac()
        if ok:      self.add_header_to_db()
        if ok:      self.copy_raw_fits()  

        #If any of these steps return false then copy to udf
        if not ok: 
            self.handle_dep_error()
        else:
            pass
        return ok


    def log_error(self, status, errcode, text=''):
        caller = inspect.stack()[1][3]
        log.error(f"func: {caller}, koaid: {self.koaid}, status: {status}, errcode:{errcode}, text:{text}")
        errdata = {'func': caller, 'status': status, 'errcode':errcode, 'text':text}
        self.errors.append(errdata)


    def check_status_db_entry(self):

        #If we didn't pass in a DB ID, we must have filepath 
        #so insert a new dep_status record and get ID
        if not self.dbid:
            query = ("insert into dep_status set "
                    f"   instrument='{self.instr}' "
                    f" , ofname='{self.filepath}' "
                    f" , status='PROCESSING' "
                    f" , creation_time='{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' ")
            log.info(query)
            result = self.db.query('koa', query, getInsertId=True)
            if result is False: 
                self.log_error('ERROR', 'QUERY_ERROR', query)
                return False
            self.dbid = result

        #Now query for it by ID (typically we are given a DB ID)
        log.info(f"Processing record ID: {self.dbid}")
        query = f"select * from dep_status where id={self.dbid}"
        row = self.db.query('koa', query, getOne=True)
        if not row:
            self.log_error('INVALID', 'DB_ID_NOT_FOUND', query)
            return False

        #If stage file is defined, use that for filepath if it exists
        self.ofname = row.get('ofname')
        self.stage_file = row.get('stage_file')
        self.filepath = row['ofname']
        if self.stage_file and os.path.isfile(self.stage_file):
            self.filepath = self.stage_file

        #if reprocessing, copy record to history and clear status columns
        if self.reprocess:
            log.info(f"Reprocessing ID# {self.dbid} with filepath {self.filepath}")
            self.copy_old_status_entry(self.dbid)
            self.reset_status_record(self.dbid)

        #update dep_status
        if not self.update_dep_status('status', 'PROCESSING'): return False
        if not self.update_dep_status('status_code', ''): return False
        if not self.update_dep_status('dep_start_time', dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')): return False

        return True


    def check_koaid_db_entry(self):

        #Query for existing KOAID record
        query = (f"select * from dep_status "
                 f" where koaid='{self.koaid}'")
        rows = self.db.query('koa', query)
        if rows is False:
            self.log_error('ERROR', 'QUERY_ERROR', query)
            return False

        #If entry exists and we are not reprocessing, return error
        if len(rows) > 0 and not self.reprocess:
            self.log_error('INVALID', 'DUPLICATE_KOAID')
            return False

        #if reprocessing, delete local files by KOAID
        if self.reprocess:
            self.delete_local_files(self.instr, self.koaid)

        #Now that KOAID check is passed, update dep_status.koaid
        if not self.update_dep_status('koaid', self.koaid): return False

        return True


    def processing_init(self):
        '''
        Perform specific initialization tasks for DEP processing.
        '''

        #define some handy utdate vars here after loading fits (dependent on set_koaid())
        self.utdate = self.get_keyword('DATE-OBS', useMap=False)
        self.utdatedir = self.utdate.replace('/', '-').replace('-', '')
        hstdate = dt.datetime.strptime(self.utdate, '%Y-%m-%d') - dt.timedelta(days=1)
        self.hstdate = hstdate.strftime('%Y-%m-%d')
        self.utc = self.get_keyword('UTC', useMap=False)
        self.utdatetime = f"{self.utdate} {self.utc[0:8]}" 

        #create output dirs (this is dependent on utdatedir above)
        self.init_dirs()

        #Update some details for this record
        if not self.update_dep_status('utdatetime', self.utdatetime): return False

        return True


    def init_dirs(self):
        #TODO: exit if existence of output/stage dirs? Maybe put override in config?

        # get the various root dirs
        self.set_root_dirs()

        # Create the output directories, if they don't already exist.
        for key, dir in self.dirs.items():
            if not os.path.isdir(dir):
                log.info(f'Creating output directory: {dir}')
                try:
                    pathlib.Path(dir).mkdir(parents=True, exist_ok=True)
                except:
                    raise Exception(f'instrument.py: could not create directory: {dir}')


    def set_root_dirs(self):
        """Sets the various rootdir subdirectories of interest"""

        rootdir = self.rootdir
        instr = self.instr
        ymd = self.utdatedir

        self.dirs = {}
        self.dirs['process'] = f"{rootdir}/{instr}"
        self.dirs['output']  = f"{rootdir}/{instr}/{ymd}"
        self.dirs['lev0']    = f"{rootdir}/{instr}/{ymd}/lev0"
        self.dirs['lev1']    = f"{rootdir}/{instr}/{ymd}/lev1"
        self.dirs['stage']   = f"{rootdir}/{instr}/stage"
        self.dirs['udf']     = f"{rootdir}/{instr}/stage/udf"


    def load_fits(self):
        '''
        Loads the fits file
        '''
        if not os.path.isfile(self.filepath):
            self.log_error('ERROR', 'FITS_NOT_FOUND')
            return False
        if (os.path.getsize(self.filepath) == 0):
            self.log_error('INVALID', 'EMPTY_FILE')
            return False

        try:
            self.fits_hdu = fits.open(self.filepath, ignore_missing_end=True)
            self.fits_hdr = self.fits_hdu[0].header
        except:
            self.log_error('INVALID', 'UNREADABLE_FITS')
            return False
        return True


    def copy_old_status_entry(self, id):

        #move to history table 
        query = (f"INSERT INTO dep_status_history "
                f" SELECT ds.* FROM dep_status as ds " 
                f" WHERE id = {id}")
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            self.log_error('ERROR', 'QUERY_ERROR', query)
            return False
        return True


    def reset_status_record(self, id):
        '''When reprocessing a record, we need to reset most columns to default.'''
        query = (f"update dep_status set "
                 f" status_code        = NULL, " 
                 f" archive_dir        = NULL, "
                 f" creation_time      = NULL, "
                 f" dep_start_time     = NULL, "
                 f" dep_end_time       = NULL, "
                 f" xfr_start_time     = NULL, "
                 f" xfr_end_time       = NULL, "
                 f" ipac_notify_time   = NULL, "
                 f" ipac_response_time = NULL, "
                 f" stage_time         = NULL, "
                 f" filesize_mb        = NULL, "
                 f" archsize_mb        = NULL, "
                 f" koaimtyp           = NULL, "
                 f" semid              = NULL "
                 f" where id = {id} ")
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            self.log_error('ERROR', 'QUERY_ERROR', query)
            return False
        return True


    def copy_raw_fits(self, invalid=False):
        '''
        Copy raw fits file so we have a copy at Keck before summit disks get wiped.
        NOTE: Stage is sort of a misnomer here since we are doing this at the end
        of processing and we don't use it except for reprocessing.  But since
        the copy can take a while, we do it at the end after ipac transfer.
        NOTE: We could move this just after processing_init() if we don't care about delay.
        '''

        #if record already has stage_file defined and it exists, no copy needed
        if self.stage_file and os.path.isfile(self.stage_file):
            log.info('Stage file defined and exists.  No copy needed.')
            return True        

        if invalid and not self.ofname:
            log.info('No raw fits file to copy.')
            return True

        #form filepath and copy
        if invalid: 
            if self.dirs: outfile = f"{self.dirs['udf']}/{self.utdatedir}{self.ofname}"
            else:         outfile = f"{self.rootdir}/{self.instr}/stage/udf/{self.ofname}"
        else:       
            outfile = f"{self.dirs['stage']}/{self.utdatedir}{self.ofname}"
        if outfile == self.filepath:
            return True

        #if outfile exists, we attach KOAID to filename
        #(This is for rare case where observer deletes file and recreates it)
        if os.path.isfile(outfile):
            log.warning(f'Stage file already exists.  Renaming with version.')
            outdir = os.path.dirname(outfile)
            base = os.path.basename(outfile)
            for i in range(2,20):
                idx = base.rfind('.')
                if idx >= 0: newbase = f"{base[:idx]}_ver{i}{base[idx:]}"
                else:        newbase = f"{base}_ver{i}"
                outfile = f"{outdir}/{newbase}"
                if not os.path.isfile(outfile):
                    break

        #copy file
        log.info(f'Copying raw fits to {outfile}')
        try:
            outdir = os.path.dirname(outfile)
            pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
            shutil.copy(self.filepath, outfile)  
        except Exception as e:
            self.log_error('ERROR', 'FILE_COPY_ERROR', outfile)
            return False
      
        #update dep_status.savepath
        self.update_dep_status('stage_file', outfile)
        return True


    def get_raw_filepath(self):
        filename = os.path.basename(self.filepath)
        outdir = self.dirs['output']
        outdir = outdir.replace(self.rootdir, '')
        outfile = f"{self.config[self.instr]['ROOTDIR']}{outdir}/{filename}"
        return outfile


    def delete_local_files(self, instr, koaid):
        '''Delete local archived output files.  This is important if we are reprocessing data.'''
        #todo: should we be saving a copy of old files?

        if not self.koaid or len(self.koaid) < 20:
            self.log_error('ERROR', 'INVALID_KOAID')
            return False

        #delete files matching KOAID*
        #todo: other files/anc/raw?
        try:
            match = f"{self.dirs['lev0']}/{self.koaid}*"
            log.info(f'Deleting local files for: {match}')
            for f in glob.glob(match):                
                log.info(f"removing file: {f}")
                os.remove(f)
        except Exception as e:
            self.log_error('ERROR', 'FILE_DELETE_ERROR')
            log.error(f"Could not delete local files for: {match}")
            return False
        return True


    def update_dep_status(self, column, value):
        """Sends command to update KOA dep_status."""

        #todo: test failure case if record does not exist.
        if value is None: query = f"update dep_status set {column}=NULL where id='{self.dbid}'"
        else:             query = f"update dep_status set {column}='{value}' where id='{self.dbid}'"
        log.info(query)
        result = self.db.query('koa', query)
        if result is False:
            self.log_error('ERROR', 'QUERY_ERROR', query)
            return False
        return True


    def validate_fits(self):
        '''Basic checks for valid FITS before proceeding with archiving'''

        #todo: There was some special logic in dep_locate for DEIMOS having a 'FCSIMGFI' keyword. See if we still need it.

        #certain text in filepath is indication that it should not be archived.
        #TODO: review this logic with Jeff
        rejects = ['mira', 'savier-protected', 'SPEC/ORP', '/subtracted', 'idf']
        for reject in rejects:
            if reject in self.filepath:
                self.log_error('INVALID', 'FILEPATH_REJECT')
                return False

        # Get fits header (check for bad header)
        try:
            if self.instr == 'NIRC2':
              header0 = fits.getheader(self.filepath, ignore_missing_end=True)
              header0['INSTRUME'] = 'NIRC2'
            else:
              header0 = fits.getheader(self.filepath)
        except:
            self.log_error('INVALID', 'UNREADABLE_FITS')
            return False

        # Construct the original file name
        filename, stat = self.construct_filename(self.instr, self.filepath, header0)
        if stat is not True:
            self.log_error('INVALID', stat)
            return False

        # Make sure constructed filename matches basename.
        basename = os.path.basename(self.filepath)
        basename = basename.replace(".fits.gz", ".fits")
        if filename != basename:
            self.log_error('INVALID', 'MISMATCHED_FILENAME', f"{filename} != {basename}")
            return False

        return True


    def construct_filename(self, instr, fitsFile, keywords):
        """Constructs the original filename from the fits header keywords"""

#TODO: CLEAN THIS UP AND GET IT WORKING
        #TODO: move this to instrument classes

        if instr in ['MOSFIRE', 'NIRES', 'NIRSPEC', 'OSIRIS']:
            try:
                outfile = keywords['DATAFILE']
                if '.fits' not in outfile:
                    outfile = ''.join((outfile, '.fits'))
                return outfile, True
            except KeyError:
                return '', 'BAD_OUTFILE'
        elif instr in ['KCWI']:
            try:
                outfile = keywords['OFNAME']
                return outfile, True
            except KeyError:
                return '', 'BAD_OUTFILE'
        else:
            try:
                outfile = keywords['OUTFILE']
            except KeyError:
                try:
                    outfile = keywords['ROOTNAME']
                except KeyError:
                    try:
                        outfile = keywords['FILENAME']
                    except KeyError:
                        return '', 'BAD_OUTFILE'

        # Get the frame number of the file
        if outfile[:2] == 'kf':   frameno = keywords['IMGNUM']
        elif instr == 'MOSFIRE':  frameno = keywords['FRAMENUM']
        elif instr == 'NIRES':    garbage, frameno = keywords['DATAFILE'].split('_')
        else:
            try:
                frameno = keywords['FRAMENO']
            except KeyError:
                try:
                    frameno = keywords['FILENUM']
                except KeyError:
                    try:
                        frameno = keywords['FILENUM2']
                    except KeyError:
                        return '', 'BAD_FRAMENO'

        #Pad frameno and construct filename
        frameno = str(frameno).strip().zfill(4)
        filename = f'{outfile.strip()}{frameno}.fits'
        return filename, True


    def create_meta(self):
        log.info('Creating metadata')

        extra_meta = {}
        koaid = self.get_keyword('KOAID')
        extra_meta[koaid] = self.extra_meta

        keydefs = self.config['MISC']['METADATA_TABLES_DIR'] + '/keywords.format.' + self.instr
        metaoutfile =  self.dirs['lev0'] + '/' + self.koaid + '.metadata.table'
        ok = metadata.make_metadata( keydefs, metaoutfile, filepath=self.outfile, 
                                     extraData=extra_meta, keyskips=self.keyskips)   
        return ok


    def create_ext_meta(self):
#TODO: TEST THIS!
        '''
        Creates IPAC ASCII formatted data files for any extended header data found.
        '''
        #todo: put in warnings for empty ext headers

        if self.instr in ('KCWI'):
            return True
        log.info(f'Making FITS extension metadata files for: {self.koaid}')

        #read extensions and write to file
        #todo: do we need to fits.open here again?
        filename = os.path.basename(self.filepath)
        hdus = fits.open(self.filepath)
        for i in range(0, len(hdus)):
            #wrap in try since some ext headers have been found to be corrupted
            try:
                hdu = hdus[i]
                if 'TableHDU' not in str(type(hdu)): continue

                #calc col widths
                dataStr = ''
                colWidths = []
                for idx, colName in enumerate(hdu.data.columns.names):
                    try:
                        fmtWidth = int(hdu.data.formats[idx][1:])
                    except:
                        fmtWidth = int(hdu.data.formats[idx][:-1])
                        if fmtWidth < 16: fmtWidth = 16
                    colWidth = max(fmtWidth, len(colName))
                    colWidths.append(colWidth)

                #add hdu name as comment
                dataStr += '\ Extended Header Name: ' + hdu.name + "\n"

                #add header
                #TODO: NOTE: Found that all ext data is stored as strings regardless of type it seems to hardcoding to 'char' for now.
                for idx, cw in enumerate(colWidths):
                    dataStr += '|' + hdu.data.columns.names[idx].ljust(cw)
                dataStr += "|\n"
                for idx, cw in enumerate(colWidths):
                    dataStr += '|' + 'char'.ljust(cw)
                dataStr += "|\n"
                for idx, cw in enumerate(colWidths):
                    dataStr += '|' + ''.ljust(cw)
                dataStr += "|\n"
                for idx, cw in enumerate(colWidths):
                    dataStr += '|' + ''.ljust(cw)
                dataStr += "|\n"

                #add data rows
                for j in range(0, len(hdu.data)):
                    row = hdu.data[j]
                    for idx, cw in enumerate(colWidths):
                        valStr = row[idx]
                        dataStr += ' ' + str(valStr).ljust(cw)
                    dataStr += "\n"

                #write to outfile
                outDir = os.path.dirname(self.filepath)
                outFile = filename.replace(endsWith, '.ext' + str(i) + '.' + hdu.name + '.tbl')
                outFilepath = outDir + outFile
                with open(outFilepath, 'w') as f:
                    f.write(dataStr)

                #Create ext.md5sum.table
                md5Prepend = self.utdatedir+'.'
                md5Outfile = f'{outDir}/{self.koaid}.ext.md5sum.table'
                log.info('Creating {}'.format(md5Outfile))
                make_dir_md5_table(outDir, None, md5Outfile, regex=f"{self.koaid}.ext\d")

            except:
                #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
                log.error(f'Could not create extended header table for ext header index {i} for file {filename}!')
                return False

        return True


    def check_koapi_send(self):
        '''
        For each unique semids processed in DQA, call function that determines
        whether to flag semids for needing an email sent to PI that there data is archived
        '''

        #check if we should update koapi_send
        semid = self.get_semid()
        sem, prog = semid.upper().split('_')
        if not semid or not prog or not sem:
            return True
        if prog == 'NONE' or prog == 'NULL' or prog == 'ENG':
            return True

        #process it
        log.info(f'check_koapi_send: {self.utdate}, {semid}, {self.instr}')
        ok = update_koapi_send.update_koapi_send(self.utdate, semid, self.instr)
        if not ok:
            #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
            log.error('check_koapi_send failed')
            return False

        #NOTE: This should not hold up archiving
        return True


    def handle_dep_error(self):

        #if for some reason we didn't record the error
        status = "ERROR"
        errcode = "UNKNOWN"
        text = ''

        #get last of errors logged for this koaid
        if self.errors:
            error = self.errors[-1]
            status  = error['status']
            errcode = error['errcode']
            text = str(self.errors)

        #log
        log.error(f'Status: {status}, Code: {errcode}, DBID: {self.dbid}, KOAID: {self.koaid}, File:{self.filepath}')
        if text: log.error(text)

        #update by dbid
        if self.dbid:
            query = (f"update dep_status set status='{status}', status_code='{errcode}' "
                     f" where id={self.dbid}")
            log.info(query)
            result = self.db.query('koa', query)
            if result is False: 
                #todo: what can we do if THIS fails??  Email admins direct?
                log.error(f'ERROR STATUS QUERY FAILED: {query}')
                return False

        #Copy to anc if INVALID
        if status == 'INVALID':
            self.copy_raw_fits(invalid=True)


    def copy_bad_file(self):
        #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
        if not self.filepath:
            log.error('No filepath to copy to ANC folder')
            return False
        try:
            outdir = self.dirs['udf']
            shutil.copy(self.filepath, outdir)  
            log.info(f"Copied invalid fits {self.filepath} to {outdir}")
        except Exception as e:
            log.error(f"Could not copy invalid fits {self.filepath} to {outdir}")
            return False
        return True


    def verify_date(self, date=''):
        """
        Verify that date value has format yyyy-mm-dd
            yyyy >= 1990
            mm between 01 and 12
            dd between 01 and 31
        """        
        #TODO: Do we need this function?
        # Verify correct format (yyyy-mm-dd or yyyy/mm/dd)
        assert date != '', 'date value is blank'
        assert re.search('\d\d\d\d[-/]\d\d[-/]\d\d', date), 'unknown date format'
        
        # Switch to yyyy-mm-dd format and split into individual elements        
        date = date.replace('/', '-')
        year, month, day = date.split('-')
        
        # Check date components
        assert int(year) >= 1990, 'year value must be 1990 or larger'
        assert int(month) >= 1 and int(month) <= 12, 'month value must be between 1 and 12'
        assert int(day) >= 1 and int(day) <= 31, 'day value must be between 1 and 31'


    def verify_utc(self, utc=''):
        """
        Verify that utc value has the format hh:mm:ss[.ss]
        hh between 0 and 24
        mm between 0 and 60
        ss between 0 and 60
        """        
        # Verify correct format (hh:mm:ss[.ss])
        if not utc: return False
        if not re.search('\d\d:\d\d:\d\d', utc): return False
        
        # Check time components       
        hour, minute, second = utc.split(':')        
        if int(hour) < 0 or int(hour) > 24: return False
        if int(minute) < 0 or int(minute) > 60: return False
        if float(second) < 0 or float(second) > 60: return False

        return True


    def is_progid_valid(self, progid):
        '''
        Check if progid is valid.
        NOTE: We allow the old style of progid without semester thru this check.
        '''
        if not progid: return False

        #get valid parts
        if   progid.count('_') > 1 : return False    
        elif progid.count('_') == 1: sem, progid = progid.split('_')
        else                       : sem = False

        #checks
        if len(progid) <= 2:      return False
        if len(progid) >= 6:      return False
        if " " in progid:         return False
        if "PROGID" in progid:    return False
        if sem and len(sem) != 5: return False

        return True


    def get_prog_inst(self, semid, default=None, isToO=False):
        '''Query the proposalsAPI and get the program institution'''
        api = self.config.get('API', {}).get('PROPAPI')
        url = api + 'ktn='+semid+'&cmd=getAllocInst&json=True'
        data = self.get_api_data(url)
        if not data or not data.get('success'):
            #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
            log.error('Unable to query API: ' + url)
            return default
        else:
            val = data.get('data', {}).get('AllocInst', default)
            return val


    def get_prog_pi(self, semid, default=None):
        '''Query for program's PI last name'''
        query = ( 'select pi.pi_lastname, pi.pi_firstname '
                  ' from koa_program as p, koa_pi as pi '
                 f' where p.semid="{semid}" and p.piID=pi.piID')
        data = self.db.query('koa', query, getOne=True)
        if not data or 'pi_lastname' not in data:
            #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
            log.error(f'Unable to get PI name for semid {semid}')
            return default
        else:
            val = data['pi_lastname'].replace(' ','')
            return val


    def get_prog_title(self, semid, default=None):
        '''Query the DB and get the program title'''
        query = f'select progtitl from koa_program where semid="{semid}"'
        data = self.db.query('koa', query, getOne=True)
        if not data or 'progtitl' not in data:
            #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
            log.error(f'Unable to get title for semid {semid}')
            return default
        else:
            return data['progtitl']


    def create_readme(self):
        '''Create a text file that indicates some meta about KOAID product delivery'''
        try:
            filepath = f"{self.dirs['lev0']}/{self.koaid}.txt"
            with open(filepath, 'w') as f:
                path = self.dirs['output']
                f.write(path + '\n')
        except Exception as e:
            self.log_error('ERROR', 'FILE_IO', filepath)
            return False
        return True


    def update_dep_stats(self):
        '''Record DEP stats before we xfr to ipac.'''
        #todo: add other column data like size, sdata_dir, etc
        if not self.update_dep_status('archive_dir', self.dirs['lev0']): return False

        filesize_mb = self.get_filesize_mb()
        if not self.update_dep_status('filesize_mb', filesize_mb): return False

        archsize_mb = self.get_archsize_mb()
        if not self.update_dep_status('archsize_mb', archsize_mb): return False

        semid = self.get_semid()
        if not self.update_dep_status('semid', semid): return False

        koaimtyp = self.get_keyword('KOAIMTYP')
        if not self.update_dep_status('koaimtyp', koaimtyp): return False

        now = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if not self.update_dep_status('dep_end_time', now): return False

        return True


    def get_filesize_mb(self):
        """Returns the archived fits size in MB"""
        bytes = os.path.getsize(self.outfile)
        return str(bytes/1e6)


    def get_archsize_mb(self):
        """Returns the archive size in MB"""
        bytes = 0
        search = f"{self.dirs['lev0']}/{self.koaid}*"
        for file in glob.glob(search):
            bytes += os.path.getsize(file)
        return str(bytes/1e6)


    def get_api_data(self, url, getOne=False, isJson=True):
        '''
        Gets data for common calls to url API requests.
        #todo: add some better validation checks 
        '''
        try:
            data = urlopen(url)
            data = data.read().decode('utf8')
            if isJson: data = json.loads(data)
            if getOne and len(data) > 0: 
                data = data[0]
            return data
        except Exception as e:
            return None


    def transfer_ipac(self):
        """
        Transfers the data set for koaid located in the output directory to its
        final archive destination.  After successful transfer of data set, 
        ingestion API (KOAXFR:INGESTAPI) is called to trigger the archiving process.
        """

        if not self.transfer:
            log.info('NOT TRANSFERRING TO IPAC.  Use --transfer flag.')
            return True

        # shorthand vars
        fromDir = self.dirs['lev0']

        # Verify that this dataset should be transferred
        query = f'select * from dep_status where id={self.dbid} and xfr_start_time is null'
        row = self.db.query('koa', query, getOne=True)
        if not row:
            log.error(f'dep_status entry not ready to transfer for {self.koaid} and dbid={self.dbid}')
            return False

        # Verify that there is a dataset to transfer
        if not os.path.isdir(fromDir):
            self.log_error('ERROR', 'NO_TRANSFER_DIR', fromDir)
            return False

        pattern = f'{self.koaid}*'
        koaidList = [f for f in fnmatch.filter(os.listdir(fromDir), pattern) if os.path.isfile(os.path.join(fromDir, f))]
        if len(koaidList) == 0:
            self.log_error('ERROR', 'NO_TRANSFER_FILES', fromDir)
            return False

        # xfr config parameters
        server = self.config['KOAXFR']['SERVER']
        account = self.config['KOAXFR']['ACCOUNT']
        toDir = self.config['KOAXFR']['DIR']
        api = self.config['KOAXFR']['INGESTAPI']

        # Configure the transfer command
        toLocation = f'{account}@{server}:{toDir}/{self.instr}/{self.utdatedir}'
        log.info(f'transferring directory {fromDir} to {toLocation}')
        log.info(f'rsync -avz --include="{pattern}" {fromDir} {toLocation}')

        # Transfer the data
        import subprocess as sp
        cmd = f'rsync -avz --include="*/" --include="{pattern}" --exclude="*" {fromDir} {toLocation}'
        xfrCmd = sp.Popen([cmd], stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
        utstring = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if not self.update_dep_status('xfr_start_time', utstring): return False
        if not self.update_dep_status('status', 'TRANSFERRING'): return False

        output, error = xfrCmd.communicate()

        # Transfer success
        if not error:
            utstring = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            if not self.update_dep_status('xfr_end_time', utstring): return False
            if not self.update_dep_status('status', 'TRANSFERRED'): return False

            # Send API request to archive the data set
            apiUrl = f'{api}instrument={self.instr}&utdate={self.utdate}&koaid={self.koaid}&ingesttype=lev0'
            if self.reprocess:
                apiUrl = f'{apiUrl}&reingest=true'
            log.info(f'sending ingest API call {apiUrl}')
            utstring = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            if not self.update_dep_status('ipac_notify_time', utstring): return False
# Need to uncomment the following when API is ready
#            apiData = self.get_api_data(apiUrl)
#            if not apiData or not data.get('apiStatus'):
#                log.error(f'error calling IPAC API {apiUrl}')
#                self.update_dep_status('status', 'ERROR')
#                self.update_dep_status('status_code', 'IPAC_NOTIFY_ERROR')
#                return False
            return True
        # Transfer error
        else:
            # Update dep_status
            self.update_dep_status('xfr_start_time', None)
            self.log_error('ERROR', 'TRANSFER_ERROR')
            return False


    def add_header_to_db(self):
        '''
        Converts the primary header into a dictionary and inserts that 
        data into the json column of the headers database table.
        '''

        d = {}
        for key in self.fits_hdr.keys():
            if key == 'COMMENT' or key == '' or key in d.keys():
                continue
            d[key] = {}
            d[key]['value'] = self.get_keyword(key)
            d[key]['comment'] = self.fits_hdr.comments[key]

        query = 'insert into headers set koaid=%s, header=%s'
        vals = (self.koaid, json.dumps(d),)
        if self.reprocess:
            query = 'update headers set header=%s where koaid=%s'
            vals = (json.dumps(d), self.koaid,)
        result = self.db.query('koa', query, values=vals)
        if result is False: 
            #todo: log_error with status 'WARN'.  How can we alert admins without marking as error?
            log.error(f'header table insert failed')
            return False

        return True

