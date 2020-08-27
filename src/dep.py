#import datetime as dt
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
from datetime import timedelta, datetime as dt
import shutil

from common import *
from envlog import *

#todo
#import create_log as cl
import logging
log = logging.getLogger('koadep')


class DEP:

    def __init__(self, instr, filepath, config, db, reprocess, tpx):

        #class inputs
        self.instr     = instr
        self.filepath  = filepath
        self.config    = config
        self.db        = db
        self.reprocess = reprocess
        self.tpx       = tpx

        #init other vars
        self.koaid = '';
        self.fits_hdu = None
        self.fits_hdr = None
        self.fits_path = None
        self.extra_meta = {}

        #other helpful vars
        self.rootdir = self.config[self.instr]['ROOTDIR']
        if self.rootdir.endswith('/'): self.rootdir = self.rootdir[:-1]


    def process(self):

        #todo: tpx confirm or any other confirm checks still needed? 
        ok = True
        if ok: ok = self.load_fits(self.filepath)
        if ok: ok = self.set_koaid()
        if ok: ok = self.processing_init()
        if ok: ok = self.check_koa_db_entry()
        if ok: ok = self.validate_fits()
        if ok: ok = self.copy_raw_fits()  
        if ok: ok = self.run_dqa()
        if ok: ok = self.write_fits() 
        if ok:      self.make_jpg()
        if ok: ok = self.create_meta()
        if ok: ok = self.xfr_ipac()


    def processing_init(self):
        '''
        Perform specific initialization tasks for DEP processing.
        '''

        #define utDate here after loading fits
        self.utDate = self.get_keyword('DATE-OBS')
        self.utDateDir = self.utDate.replace('/', '-').replace('-', '')

        #check and create dirs
        self.init_dirs()

        #create README (output dir with everything before /koadata##/... stripped off)
        readmeFile = self.dirs['output'] + '/README';
        with open(readmeFile, 'w') as f:
            path = self.dirs['output']
            f.write(path + '\n')

        return True


    def init_dirs(self):
        #TODO: exit if existence of output/stage dirs? Maybe put override in config?

        # get the various root dirs
        self.set_root_dirs()

        # Create the output directories, if they don't already exist.
        # Unless this is a full pyDEP run, in which case we exit with warning
        for key, dir in self.dirs.items():
            if os.path.isdir(dir):
                log.info(f'Output directory exists: {dir}')
            else:
                try:
                    os.makedirs(dir)
                except:
                    raise Exception(f'instrument.py: could not create directory: {dir}')

        # Additions for NIRSPEC
        # TODO: move this to instr_nirspec.py?
        if self.instr == 'NIRSPEC':
            for dir in ['scam', 'spec']:
                newdir = self.dirs['lev0'] + '/' + dir
                if not os.path.isdir(newdir):
                    os.mkdir(newdir)


    def set_root_dirs(self):
        """Sets the various rootdir subdirectories of interest"""

        rootdir = self.rootdir
        instr = self.instr.upper()
        ymd = self.utDateDir

        dirs = {}
        dirs['process'] = ''.join((rootdir, '/', instr))
        dirs['output']  = ''.join((rootdir, '/', instr, '/', ymd))
        dirs['lev0']    = ''.join((rootdir, '/', instr, '/', ymd, '/lev0'))
        dirs['lev1']    = ''.join((rootdir, '/', instr, '/', ymd, '/lev1'))
        dirs['anc']     = ''.join((rootdir, '/', instr, '/', ymd, '/anc'))
        dirs['udf']     = ''.join((dirs['anc'], '/udf'))
        self.dirs = dirs


    def load_fits(self, filepath):
        '''
        Sets the current FITS file we are working on.  Clears out temp fits variables.
        '''
        try:
            self.fits_hdu = fits.open(filepath, ignore_missing_end=True)
            self.fits_hdr = self.fits_hdu[0].header
            self.fits_path = filepath
        except:
            log.error('load_fits_file: Could not read FITS file "' + filepath + '"!')
            return False
        return True


    def check_koa_db_entry(self, ):

        #If we are not updating DB, just return 
        if not self.tpx:
            log.info("TPX is off.  Not creating DB entry.") 
            return True

        # See if entry exists
        query = f'select count(*) as num from dep_status where instr="{self.instr}" and koaid="{self.koaid}"'
        check = db.query('koa', query, getOne=True)
        if check is False:
            if log: log.error(f'Could not query dep_status for: {self.instr}, {self.koaid}, {column}, {value}')
            return False

        #if entry exists and not reprocessing, fail
        if int(check['num']) > 0 and not self.reprocess:
            log.error(f"Record already exists for {self.instr} {self.koaid}.  Rerun with --reprocess to override.")
            return False

        #if entry exists and reprocessing, delete record
        if int(check['num']) > 0 and self.reprocess:
            log.info(f"Reprocessing {self.instr} {self.koaid}")
            self.delete_status_record(self.instr, self.koaid)
            self.delete_local_files(self.instr, self.koaid)

        #We always insert a new dep_status record
        query = ("insert into dep_status set ",
                f" instr='{self.instr}' ",
                f" koaid='{self.koaid}' ",
                f" filepath='{self.filepath}' ",
                f" dep_step='{__name__}' ",
                f" dep_status='PROGRESS' ",
                f" creation_time='{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' ")
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            log.error(f'{__name__} failed')
            return False
        return True


    def delete_status(self, instr, koaid):
        query = f"delete from dep_status where instr='{instr}' and koaid='{koaid}' "
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            log.error(f'{__name__} failed')
            return False
        return True


    def delete_local_files(self, instr, koaid):
        '''Delete local archived output files.  This is important if we are reprocessing data.'''
        #todo: finish this
        pass


    def update_koatpx(self, column, value):
        """Sends command to update KOA dep_status."""

        #If we are not updating DB, just return 
        if not self.tpx: return True

        #todo: test failure case if record does not exist.
        query = f'update dep_status set {column}="{value}" where instr="{self.instr}" and koaid="{self.koaid}"'
        log.info(query)
        if db.query('koa', query) is False:
            if log: log.error(f'update_koatpx failed for: {self.instr}, {self.koaid}, {column}, {value}')
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
                self.copy_bad_file(self.instr, self.filepath, f"Filepath contains '{reject}'")
                return False

        # check for empty file
        if (os.path.getsize(self.filepath) == 0):
            self.copy_bad_file(self.instr, self.filepath, "Empty file")
            log.error(f"Empty file: {self.filepath}")
            return False

        # Get fits header (check for bad header)
        try:
            if self.instr == 'NIRC2':
              header0 = fits.getheader(self.filepath, ignore_missing_end=True)
              header0['INSTRUME'] = 'NIRC2'
            else:
              header0 = fits.getheader(self.filepath)
        except:
            self.copy_bad_file(self.instr, self.filepath, "Unreadable header")
            return False

        # Construct the original file name
        filename, ok = self.construct_filename(self.instr, self.filepath, header0)
        if not ok:
          self.copy_bad_file(self.instr, self.filepath, 'Bad Header')
          return False

        # Make sure constructed filename matches basename.
        basename = os.path.basename(self.filepath)
        basename = basename.replace(".fits.gz", ".fits")
        if filename != basename:
          copy_bad_file(self.instr, self.filepath, 'Mismatched filename')
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
                copy_bad_file(instr, fitsFile, 'Bad Outfile')
                return '', False
        elif instr in ['KCWI']:
            try:
                outfile = keywords['OFNAME']
                return outfile, True
            except KeyError:
                copy_bad_file(instr, fitsFile, 'Bad Outfile')
                return '', False
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
                        copy_bad_file(instr, fitsFile, 'Bad Outfile')
                        return '', False

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
                        copy_bad_file(instr, fitsFile, 'Bad Frameno')
                        return '', False

        #Pad frameno and construct filename
        frameno = str(frameno).strip().zfill(4)
        filename = f'{outfile.strip()}{frameno}.fits'
        return filename, True


    def copy_bad_file(self, instr, filepath, reason):
        #todo: What are we doing with bad files?
        log.error(f"Invalid FITS.  Reason: {reason}.  File: {filepath}")
        pass


    def copy_raw_fits(self):
        '''Make a permanent read-only copy of the FITS file on Keck storageserver'''

        #If we are not updating DB, just return 
        if not self.tpx:
            log.info("TPX is off.  Not copying raw fits to storageserver.") 
            return True

        if self.reprocess:
            log.info("Reprocessing.  Not copying raw fits to storageserver.") 
            return True


        #todo: finish this
        #todo: update savepath


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
            log.error(f'Unable to get title for semid {semid}')
            return default
        else:
            return data['progtitl']


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

