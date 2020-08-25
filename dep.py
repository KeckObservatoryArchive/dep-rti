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

    def __init__(self, instr, filepath, config, db, reprocess, reprocess):

        #class inputs
        self.instr     = instr
        self.filepath  = filepath
        self.config    = config
        self.db        = db
        self.reprocess = reprocess

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
        if ok: ok = self.load_fits(filepath)
        if ok: ok = self.set_koaid()
        if ok: ok = self.processing_init()
        if ok: ok = self.check_koa_db_entry()
        if ok: ok = self.copy_fits()  #todo: update savepath
        if ok: ok = self.validate_fits()
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
                if key != 'process':
                    raise Exception('instrument.py: Staging and/or output directories already exist')
            else:
                try:
                    os.makedirs(dir)
                except:
                    raise Exception('instrument.py: could not create directory: {}'.format(dir))

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
        dirs['stage']   = ''.join((rootdir, '/stage/', instr, '/', ymd))
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


    def check_koa_db_entry():

        #If we are not updating DB, just return 
        if not self.tpx: return True

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
                f" creation_time='{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}' ")
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            log.error(f'{__name__} failed')
            return False
        return True


    def delete_status(instr, koaid):
        query = f"delete from dep_status where koaid='{self.koaid}' "
        log.info(query)
        result = self.db.query('koa', query)
        if result is False: 
            log.error(f'{__name__} failed')
            return False
        return True


    def update_koatpx(column, value):
        """Sends command to update KOA data."""

        #If we are not updating DB, just return 
        if not self.tpx: return True

        #todo: test failure case if record does not exist.
        query = f'update dep_status set {column}="{value}" where instr="{self.instr}" and koaid="{self.koaid}"'
        log.info(query)
        if db.query('koa', query) is False:
            if log: log.error(f'update_koatpx failed for: {self.instr}, {self.koaid}, {column}, {value}')
            return False
        return True


    def verify_date(date=''):
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


    def verify_utc(utc=''):
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

