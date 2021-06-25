"""
The parent class for all the instruments to streamline big picture things
Contains basic keyword values common across all the instruments
Children will contain the instrument specific values
"""

#import datetime as dt
import os
from common import *
from astropy.io import fits
import datetime as dt
from envlog import *
import shutil
import json
import numpy as np
import re
import math
import traceback

import db_conn
import dep

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from PIL import Image
from astropy.visualization import ZScaleInterval, AsinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize

import logging
log = logging.getLogger('koa_dep')



class Instrument(dep.DEP):

    def __init__(self, instr, filepath, reprocess, transfer, dbid=None):

        super().__init__(instr, filepath, reprocess, transfer, dbid)

        # Common keywords used in code that can be mapped to actual keyword per instrument 
        # so a call to get_keyword can be used generically.  Overwrite values in instr_[instr].py
        # NOTE: An array may be used to denote an ordered list of possible keywords to look for.
        self.keymap = {}
        self.keymap['INSTRUME'] = 'INSTRUME'
        self.keymap['UTC']      = 'UTC'
        self.keymap['DATE-OBS'] = 'DATE-OBS'
        self.keymap['SEMESTER'] = 'SEMESTER'
        self.keymap['OFNAME']   = 'OFNAME'
        self.keymap['FRAMENO']  = 'FRAMENO'
        self.keymap['OUTDIR']   = 'OUTDIR'

        # Values to be populated by subclass
        self.prefix    = ''
        self.keyskips  = []


    #abstract methods that must be implemented by inheriting classes
    def get_dir_list(self) : raise NotImplementedError("Abstract method not implemented!")
    def get_prefix(self)   : raise NotImplementedError("Abstract method not implemented!")
    def set_koaimtyp(self) : raise NotImplementedError("Abstract method not implemented!")


    def run_dqa_funcs(self, funcs):
        '''
        Run a list of functions by name.  If the function returns False or throws exception,
        check if it is a critical function before breaking processing.
        '''
        for f in funcs:
            name = f.get('name')
            crit = f.get('crit')
            args = f.get('args', {})
            log.info(f'Running DQA function: {name}')
            try: 
                ok = getattr(self, name)(**args)
            except Exception as e: 
                etype = 'ERROR' if crit else 'WARN'
                self.log_error(etype, 'CODE_ERROR', traceback.format_exc())
                ok = False
            if not ok and crit:
                return False
        return True


    def get_keyword(self, keyword, useMap=True, default=None, ext=0):
        '''
        Gets keyword value from the FITS header as defined in keymap class variable.  
        '''
        
        # check header ext exists
        if not self.fits_hdu[ext].header:
             raise Exception('get_keyword: ERROR: no FITS header loaded')
             return default

        #if keyword is mapped, then use mapped value(s)        
        if useMap:    
            if isinstance(keyword, str) and keyword in self.keymap:
                keyword = self.keymap[keyword]

        #We allow an array of mapped keys, so if keyword is still a string then put in array
        mappedKeys = keyword
        if isinstance(mappedKeys, str):
            mappedKeys = [mappedKeys]

        #loop
        for mappedKey in mappedKeys:
            val = self.fits_hdu[ext].header.get(mappedKey)
            if val != None and not isinstance(val, fits.Undefined): return val

        #return None if we didn't find it
        return default


    def set_keyword(self, keyword, value, comment='', useMap=False, ext=0):
        '''
        Sets keyword value in FITS header.
        NOTE: Mapped values are only used if "useMap" is set to True, otherwise keyword name is as provided.
        '''

        # check for loaded fits_hdr
        if not self.fits_hdu[ext].header:
             raise Exception('get_keyword: ERROR: no FITS header loaded')
             return default

        # We allow an array of mapped keys, so if keyword is array, then use first value
        if useMap:
            if keyword in self.keymap:
                keyword = self.keymap[keyword]

        #NOTE: If keyword is mapped to an array of key values, the first value will be used.
        if isinstance(keyword, list):
            keyword = keyword[0]

        #handle infinite value
        if value == math.inf:
            log.warning(f'set_keyword: keyword {keyword} value is infinite.  Setting to null.')
            return None

        #ok now we can update
        (self.fits_hdu[ext].header).update({keyword : (value, comment)})


    def set_koaid(self):
        '''
        Create and add KOAID to header if it does not already exist
        '''

        #skip if it exists
        koaid = self.get_keyword('KOAID', False)
        if koaid != None: 
            self.koaid = koaid.replace('.fits', '')
            return True

        #make it
        koaid = self.make_koaid()
        if not koaid: 
            self.log_error("KOAID_CREATE_FAIL")
            return False

        #save it
        koaid_meta = koaid + '.fits'
        self.set_keyword('KOAID', koaid_meta, 'KOA: Data file name')
        self.koaid = koaid
        return True


    def make_koaid(self):
        """
        Function to create the KOAID for the current loaded FITS file
        Returns the koaid and TRUE if the KOAID is successfully created
        """

        # Get the prefix for the correct instrument and configuration
        self.set_instr()
        self.prefix = self.get_prefix()
        if self.prefix == '': return False

        # Extract the UTC time and date observed from the header
        self.set_utc()
        utc = self.get_keyword('UTC', useMap=False)
        if utc == None: return False

        self.set_dateObs()
        dateobs = self.get_keyword('DATE-OBS', useMap=False)
        if dateobs == None: return False

        # Create a timedate object using the string from the header
        try:    utc = dt.datetime.strptime(utc, '%H:%M:%S.%f')
        except: return False

        # Get total seconds and hundredths
        totalSeconds = str((utc.hour * 3600) + (utc.minute * 60) + utc.second)
        hundredths = str(utc.microsecond)[0:2]

        # Remove any date separators from the date
        dateobs = dateobs.replace('-','')
        dateobs = dateobs.replace('/','')

        # Create the KOAID from the parts
        koaid = f'{self.prefix}.{dateobs}.{totalSeconds.zfill(5)}.{hundredths.zfill(2)}'
        return koaid


    def is_engineering(self):
        '''Check for indicators that this is definitely engineering data.'''
            
        #keyword values that indicate ENG
        keyvals = {
            'PROGNAME': [
                'eng'
            ],
            'OUTDIR': [
                'kcwieng', 
                'kcwirun', 
                'hireseng',
                'nspeceng', 
                'nirc2eng', 
                'dmoseng', 
                'lriseng', 
                'esieng',
                'nireseng', 
                'osrseng',
                'osiriseng',                            
                'moseng'   
            ],
            'OBSERVER': [
                'keck ipdm',
                #'engineering',
                'nirspec'
            ],
            'CAMERA': [
                'fpc'
            ]
        }
        for kw, vals in keyvals.items():
            hdrval = self.get_keyword(kw, default='')
            for val in vals:
                if val in hdrval:
                    return True
        if self.check_zero_propint(): return True
        return False


    def get_instr(self):
        """
        Method to extract the name of the instrument from the INSTRUME keyword value
        """

        # Extract the Instrume value from the header as lowercase
        instr = self.get_keyword('INSTRUME')
        if (instr == None) : return ''
        instr = instr.lower()

        # Split the value up into an array 
        instr = instr.split(' ')

        # The instrument name should always be the first value
        instr = instr[0].replace(':','')
        return instr

 
    def set_instr(self):
        '''
        Check that value(s) in header indicates this is valid instrument and fixes if needed.
        '''

        ok = False

        #direct match (or starts with match)?
        instrume = self.get_keyword('INSTRUME')
        if instrume and instrume.startswith(self.instr):
            if instrume != self.instr:
                self.set_keyword('INSTRUME', self.instr, 'KOA: Instrument')
            ok = True

        #mira not ok
        outdir = self.get_keyword('OUTDIR')
        if (outdir and '/mira' in outdir) : ok = False

        #No DCS keywords, check others
        if (not ok):
            filname = self.get_keyword('FILNAME')
            if (filname and self.instr in filname): ok = True

            outdir = self.get_keyword('OUTDIR')
            if (outdir and self.instr in outdir.upper()): ok = True
            if (outdir and 'sdata100' in outdir and outdir.endswith('fcs')): ok = True
            currinst = self.get_keyword('CURRINST')
            if (currinst and self.instr == currinst): ok = True

            #if fixed, then update 'INSTRUME' in header
            if ok:
                self.set_keyword('INSTRUME', self.instr, 'KOA: Fixing INSTRUME keyword')
                log.info('set_instr: fixing INSTRUME value')

        #log err
        if (not ok):
            self.log_error('SET_INSTR_ERROR')

        return ok



    def set_dateObs(self):
        '''
        Checks to see if we have a DATE-OBS keyword, and if it needs to be fixed or created.
        '''

        #try to get from header (unmapped or mapped)
        dateObs = self.get_keyword('DATE-OBS', False)
        if dateObs == None: dateObs = self.get_keyword('DATE-OBS')

        #validate
        valid = False
        if dateObs: 
            dateObs = str(dateObs) #NOTE: sometimes we can get a number
            dateObs = dateObs.strip()
            valid = re.search(r'^\d\d\d\d[-]\d\d[-]\d\d', dateObs)
            #fix slashes?
            if not valid and '/' in dateObs:
                orig = dateObs
                day, month, year = dateObs.split('/')
                if int(year)<50: year = '20' + year
                else:            year = '19' + year
                dateObs = year + '-' + month + '-' + day
                self.set_keyword('DATE-OBS', dateObs, 'KOA: Value corrected (' + orig + ')')
                log.warning('set_dateObs: fixed DATE-OBS format (orig: ' + orig + ')')
                valid = True

        #if we couldn't match valid pattern, then build from file last mod time
        #note: converting to universal time (+10 hours)
        if not valid:
            lastMod = os.stat(self.filepath).st_mtime
            dateObs = dt.datetime.fromtimestamp(lastMod) + dt.timedelta(hours=10)
            dateObs = dateObs.strftime('%Y-%m-%d')
            self.set_keyword('DATE-OBS', dateObs, 'KOA: Observing date')
            log.warning('set_dateObs: set DATE-OBS value from FITS file time')

        # If good match, just take first 10 chars (some dates have 'T' format and extra time)
        if len(dateObs) > 10:
            orig = dateObs
            dateObs = dateObs[0:10]
            self.set_keyword('DATE-OBS', dateObs, 'KOA: Value corrected (' + orig + ')')
            log.warning('set_dateObs: fixed DATE-OBS format (orig: ' + orig + ')')

        return True
       


    def set_utc(self):
        '''
        Checks to see if we have a UTC time keyword, and if it needs to be fixed or created.
        '''

        #try to get from header unmapped and mark if update needed
        update = False
        utc = self.get_keyword('UTC', False)
        if utc == None: update = True

        #try to get from header mapped
        if utc == None:
            utc = self.get_keyword('UTC')
        #validate
        valid = False
        if utc: 
            utc = str(utc) #NOTE: sometimes we can get a number
            utc = utc.strip()
            valid = self.verify_utc(utc)

        #if we couldn't match valid pattern, then build from file last mod time
        #note: converting to universal time (+10 hours)
        if not valid:
            lastMod = os.stat(self.filepath).st_mtime
            utc = dt.datetime.fromtimestamp(lastMod) + dt.timedelta(hours=10)
            utc = utc.strftime('%H:%M:%S.00')
            update = True
            log.warning('set_utc: set UTC value from FITS file time')
        #update/add if need be
        if update:
            self.set_keyword('UTC', utc, 'KOA: UTC keyword corrected')
        return True



    def set_ut(self):

        #skip if it exists
        if self.get_keyword('UT', False) != None: return True

        #get utc from header
        utc = self.get_keyword('UTC')
        if utc == None: 
            self.log_warn("SET_UT_ERROR")
            return False

        #copy to UT
        self.set_keyword('UT', utc, 'KOA: Observing time')
        return True



    def get_outdir(self):
        '''
        Returns outdir if keyword exists, else derive from filename
        '''

        #return by keyword index if it exists
        outdir = self.get_keyword('OUTDIR')
        if (outdir != None) : return outdir

        #Returns the OUTDIR associated with the filename, else returns None.
        #OUTDIR = [/s]/sdata####/account/YYYYmmmDD
        #todo: should we look for '/s/' and subtract one from index?
        #NOTE: for reprocessing old data that doesn't have OUTDIR keyword, this matches
        #on /stage/ or /storageserver/ instead of /s/, which still gets the job done.  not ideal.
        try:
            filename = self.filepath
            start = filename.find('/s')
            end = filename.rfind('/')
            return filename[start:end]
        except:
            #todo: really return "None"?
            return "None"



    def get_fileno(self):

        #todo: do we need this function instead of using keyword mapping?  see subclass set_frameno
        keys = self.fits_hdr

        fileno = keys.get('FILENUM')
        if (fileno == None): fileno = keys.get('FILENUM2')
        if (fileno == None): fileno = keys.get('FRAMENO')
        if (fileno == None): fileno = keys.get('IMGNUM')
        if (fileno == None): fileno = keys.get('FRAMENUM')

        return fileno


    def set_semester(self):
        """
        Determines the Keck observing semester from the DATE-OBS keyword in header
        and updates the SEMESTER keyword in header.

        semester('2017-08-01') --> 2017A
        semester('2017-08-02') --> 2017B

        A = Feb. 2 to Aug. 1 (UT)
        B = Aug. 2 to Feb. 1 (UT)
        """

        #special override via command line option
        assign_progname = self.config.get('MISC', {}).get('ASSIGN_PROGNAME')
        if assign_progname:
            utc = self.get_keyword('UTC')
            progname = get_progid_assign(assign_progname, utc)
            if '_' in progname and is_progid_valid(progname):
                semester, progid = progname.split('_')
                self.set_keyword('SEMESTER', semester, 'Calculated SEMESTER from PROGNAME')
                log.info(f"set_semester: Set SEMESTER to '{semester}' from ASSIGN_PROGNAME '{progname}'")
                return True

        #special override assign using PROGNAME
        progname = self.get_keyword('PROGNAME', default='')
        if '_' in progname and is_progid_valid(progname):
            semester, progid = progname.split('_')
            self.set_keyword('SEMESTER', semester, 'Calculated SEMESTER from PROGNAME')
            log.info(f"set_semester: Set SEMESTER to '{semester}' from PROGNAME '{progname}'")
            return True

        #normal assign using DATE-OBS and UTC
        else:
            dateObs = self.get_keyword('DATE-OBS')
            utc     = self.get_keyword('UTC')
            if not dateObs or not utc:
                self.log_error('SET_SEMESTER_FAIL')
                return False

            #Slightly unintuitive, but get utc datetime obj and subtract 10 hours to convert to HST
            #and another 10 for 10 am cutoff as considered next days observing.
            d = dt.datetime.strptime(dateObs+' '+utc, "%Y-%m-%d %H:%M:%S.%f")
            d = d - dt.timedelta(hours=20)

            #define cutoffs and see where it lands
            #NOTE: d.year is wrong when date is Jan 1 and < 20:00:00, 
            #but it doesn't matter since we assume 'B' which is correct for Jan1 
            semA = dt.datetime.strptime(f'{d.year}-02-01 00:00:00.00', "%Y-%m-%d %H:%M:%S.%f")
            semB = dt.datetime.strptime(f'{d.year}-08-01 00:00:00.00', "%Y-%m-%d %H:%M:%S.%f")
            sem = 'B'
            if d >= semA and d < semB: sem = 'A'

            #adjust year if january
            year = d.year
            if d.month == 1: year -= 1

            semester = f'{year}{sem}'
            self.set_keyword('SEMESTER', semester, 'Calculated SEMESTER from DATE-OBS')

            return True


    def get_progid_from_schedule(self):
        '''Try to set PROGID from the information in the telescope scheduel'''

        #requires UTC value
        ut = self.get_keyword('UTC')
        if not ut: return 'NONE'
        ut = ut.split(':')
        ut = int(ut[0]) + (int(ut[1])/60.0)

        #split night information based on OUTDIR
        splitNight = -1 
        splitMap = ['SKIP', '_A', '_B', '_C', '_D', '_E']
        outdir = self.get_keyword('OUTDIR')
        #is entry in splitMap found in OUTDIR?
        splitLoc = [i for i in splitMap if outdir.endswith(i)]
        if len(splitLoc) == 1:
            splitNight = splitMap.index(splitLoc[0])

        #get schedule information
        api = self.config['API']['TELAPI']
        url = f"{api}cmd=getSchedule&date={self.hstdate}&telnr={self.telnr}&instr={self.instr}"
        log.info(f'checking schedule for PROGID: {url}')
        data = self.get_api_data(url)
        if data:
            if isinstance(data, dict):
                data = [data]
            if len(data) == 1:
                log.info(f"using the only scheduled entry: {data[0]['ProjCode']}")
                return data[0]['ProjCode']
            for num, entry in enumerate(data):
                #if there was an OUTDIR match above, use it
                if splitNight > -1:
                    if splitNight == num+1:
                        log.info(f"using schedule entry by OUTIDR: {entry['ProjCode']}")
                        return entry['ProjCode']
                    else:
                        continue
                #check if UTC between schedule start/end
                start = entry['StartTime'].split(':')
                start = int(start[0]) + (int(start[1])/60.0)
                end = entry['EndTime'].split(':')
                end = int(end[0]) + (int(end[1])/60.0)
                if ut >= start and ut <= end:
                    log.info(f"using schedule entry by UTC: {entry['ProjCode']}")
                    return entry['ProjCode']
        return 'NONE'


    def get_missing_progid(self):
        #todo: do simple progid assigment
        return "NONE"


    def set_prog_info(self):
        '''Set PROG* keywords'''

        #Get PROGNAME from header and use for PROGID
        #If not found, then do simple assignment by time/observer/outdir(eng).
        progid = self.get_keyword('PROGNAME')
        if not progid:
            #check telescope schedule
            progid = self.get_progid_from_schedule()
            print('----', progid)
            if not progid:
                #todo: (assign NONE if cannot determine)
                progid = self.get_missing_progid()

        #valid progname?
        #todo: Make sure we are getting the full semid with underscore
        valid = self.is_progid_valid(progid)
        if self.is_engineering():
            progid = 'ENG'
            valid = True
        if not valid:
            self.log_warn('INVALID_PROGID', str(progid))
        progid = progid.strip().upper()

        #add semester?
        if '_' in progid: 
            sem, prog = progid.split('_')
        else:
            sem = self.get_keyword('SEMESTER')
            prog = progid

        #try to assign PROG* keywords from progname
        if not valid:
            progid   = 'NONE'
            progpi   = 'NONE'
            proginst = 'NONE'
            progtitl = ''
        else:
            progid = progid.strip().upper()
            if progid == 'ENG':
                proginst = 'KECK'
                progpi   = self.instr.lower() + 'eng'
                progtitl = self.instr.upper() + ' Engineering'
            else:
                semid = sem + '_' + prog
                progid   = prog #NOTE: We store without semester in header
                progpi   = self.get_prog_pi   (semid, 'NONE')
                proginst = self.get_prog_inst (semid, 'NONE')
                progtitl = self.get_prog_title(semid, '')

        #update header
        self.set_keyword('PROGID'  , progid,   'KOA: Program ID')
        self.set_keyword('PROGPI'  , progpi,   'KOA: Program principal investigator')
        self.set_keyword('PROGINST', proginst, 'KOA: Program institution')

        #enocde unicode chars in progtitl and divide into length 50 (+20 for comments) chunks
        progtitl = progtitl.encode('ascii', errors='xmlcharrefreplace').decode('utf8')
        progtl1 = progtitl[0:50]
        progtl2 = progtitl[50:100]
        progtl3 = progtitl[100:150]
        self.set_keyword('PROGTL1',  progtl1, 'Program title 1')
        self.set_keyword('PROGTL2',  progtl2, 'Program title 2')
        self.set_keyword('PROGTL3',  progtl3, 'Program title 3')

        #NOTE: PROGTITL goes in metadata but not in header so we store in temp dict for later
        self.extra_meta['PROGTITL'] = progtitl
        
        return True


    def set_propint(self):
        '''
        Set proprietary period length.
        NOTE: This must come after set_prog_info and set_semester is called
        '''

        # Lookup PROP value via API (default to 18 otherwise)
        progid = self.fits_hdr.get('PROGID').upper()
        if not progid or progid == 'NONE':
            propint = 18
        elif progid == 'ENG':
            propint = 18
        else:
            semid = self.get_semid()
            api = self.config.get('API', {}).get('PROPAPI')
            url = api + 'ktn='+semid+'&cmd=getApprovedPP&json=True'
            data = self.get_api_data(url)
            if not data or not data.get('success'):
                self.log_warn('API_ERROR', url)
                propint = 18
            else:
                propint = data.get('data', {}).get('ProprietaryPeriod', 18)

        #NOTE: PROPINT goes in metadata but not in header so we store in temp dict for later
        self.extra_meta['PROPINT'] = propint

        # NEW POLICY per DKOA-82: Propint=0 for PROGID=ENG and KOAIMTYP=calib
        try:
            if self.check_zero_propint():
                log.info(f"Changing PROPINT from {self.extra_meta['PROPINT']} to 0")
                self.extra_meta['PROPINT'] = 0
        except Exception as e:
            self.log_error('CHECK_ZERO_PROPINT', str(e))

        return True


    def check_zero_propint(self):
        '''Check if we should zero out PROPINT based on new policy defined in DKOA-82.'''

        koaimtyp = self.get_keyword('KOAIMTYP')
        is_cal = koaimtyp not in ('object', 'unknown')

        has_target = self.has_target_info()

        utc = self.get_keyword('UTC')
        is_daytime = self.is_daytime(utc)

        return (is_cal and is_daytime and not has_target)


    def has_target_info(self):
        '''
        Does this fits have sensitive target info?
        NOTE: Default is to assume true unless proven otherwise
        See instr subclass overrides.
        '''
        return True


    def is_daytime(self, utc):
        '''Is the UTC time during the day?'''
        url = f"{self.config['API']['METAPI']}date={self.utdate}"
        suntimes = self.get_api_data(url, getOne=True)
        sunrise = suntimes['sunrise']
        sunset  = suntimes['sunset']
        tm         = dt.datetime.strptime(utc,     '%H:%M:%S.%f').time()
        sunset_tm  = dt.datetime.strptime(sunset,  '%H:%M').time()
        sunrise_tm = dt.datetime.strptime(sunrise, '%H:%M').time()
        is_daytime = (tm < sunset_tm or tm > sunrise_tm)
        return is_daytime


    def set_datlevel(self, level):
        '''
        Adds "DATLEVEL" keyword to header
        '''
        self.set_keyword('DATLEVEL' , level, 'KOA: Data reduction level')
        return True


    def set_dqa_date(self):
        """
        Adds date timestamp for when the DQA module was run
        """
        dqa_date = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%dT%H:%M:%S')
        self.set_keyword('DQA_DATE', dqa_date, 'KOA: Data quality assess time')
        return True


    def set_dqa_vers(self):
        '''
        Adds DQA version keyword to header
        '''
        version = self.config['INFO']['DEP_VERSION']
        self.set_keyword('DQA_VERS', version, 'KOA: Data quality assess code version')
        return True


    def set_image_stats(self):
        '''
        Adds mean, median, std keywords to header
        '''

        image = self.fits_hdu[0].data     
        imageStd    = float("%0.2f" % np.std(image))
        imageMean   = float("%0.2f" % np.mean(image))
        imageMedian = float("%0.2f" % np.median(image))

        self.set_keyword('IMAGEMN' ,  imageMean,   'KOA: Image data mean')
        self.set_keyword('IMAGESD' ,  imageStd,    'KOA: Image data standard deviation')
        self.set_keyword('IMAGEMD' ,  imageMedian, 'KOA: Image data median')

        return True


    def set_npixsat(self, satVal=None, ext=0):
        '''
        Determines number of saturated pixels and adds NPIXSAT to header
        '''
        if satVal == None:
            satVal = self.get_keyword('SATURATE')
        if satVal == None:
            log.warning("set_npixsat: Could not find SATURATE keyword")
        else:
            image = self.fits_hdu[ext].data     
            pixSat = image[np.where(image >= satVal)]
            nPixSat = len(image[np.where(image >= satVal)])
            self.set_keyword('NPIXSAT', nPixSat, 'KOA: Number of saturated pixels',ext=ext)
        return True


    def get_oa(self, hstdate, telnr):
        '''Gets OA value from API for given date and telnr.'''

        url = f"{self.config['API']['TELAPI']}cmd=getNightStaff&date={hstdate}&telnr={telnr}"
        log.info(f'retrieving night staff info: {url}')
        data = self.get_api_data(url)
        oa = 'None'
        if data:
            if isinstance(data, dict):
                data = [data]
            for entry in data:
                if entry['Type'] == 'oa' or entry['Type'] == 'oar':
                    oa = entry['Alias']
        return oa


    def set_oa(self):
        '''
        Adds observing assistant name to header
        '''
        oa = self.get_oa(self.hstdate, self.telnr)
        if oa == 'None':
            self.log_warn("SET_OA_ERROR", url)
        else:
            self.set_keyword('OA', oa, 'KOA: Observing Assistant name')
        return True


    def set_ofName(self):
        """
        Adds OFNAME keyword to header 
        """

        #get value
        ofName = self.get_keyword('OFNAME')
        if (ofName == None): 
            self.log_warn('SET_OFNAME_FAIL')
            return False

        #add *.fits to output if it does not exist (to fix old files)
        if (ofName.endswith('.fits') == False) : ofName += '.fits'

        #update
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')
        return True


    def set_weather(self):
        '''
        Adds all weather related keywords to header.
        NOTE: DEP should not exit if weather files are not found
        '''

        #get input vars
        dateobs = self.get_keyword('DATE-OBS')
        utc     = self.get_keyword('UTC')

        #get data but continue even if there were errors for certain keywords
        data, errors, warns = envlog(self.telnr, dateobs, utc)
        if type(data) is not dict: 
            self.log_warn('WEATHER_DATA_FAIL')
            return True
        if len(errors) > 0:
            self.log_warn('EPICS_ARCHIVER_ERROR', str(errors))
        if len(warns) > 0:
            log.info(f"EPICS archiver warn {dateobs} {utc}: {str(warns)}")

        #set keywords
        self.set_keyword('WXDOMHUM' , data['wx_domhum'],    'KOA: Weather dome humidity')
        self.set_keyword('WXDOMTMP' , data['wx_domtmp'],    'KOA: Weather dome temperature')
        self.set_keyword('WXDWPT'   , data['wx_dewpoint'],  'KOA: Weather dewpoint')
        self.set_keyword('WXOUTHUM' , data['wx_outhum'],    'KOA: Weather outside humidity')
        self.set_keyword('WXOUTTMP' , data['wx_outtmp'],    'KOA: Weather outside temperature')
        self.set_keyword('WXPRESS'  , data['wx_pressure'],  'KOA: Weather pressure')
        self.set_keyword('WXWNDIR'  , data['wx_winddir'],   'KOA: Weather wind direction')
        self.set_keyword('WXWNDSP'  , data['wx_windspeed'], 'KOA: Weather wind speed')
        self.set_keyword('WXTIME'   , data['wx_time'],      'KOA: Weather measurement time')
        self.set_keyword('GUIDFWHM' , data['guidfwhm'],     'KOA: Guide star FWHM value')
        self.set_keyword('GUIDTIME' , data['fwhm_time'],    'KOA: Guide star FWHM measure time')
        return True


    def set_telnr(self):
        '''
        Gets telescope number for instrument via API
        #todo: Replace API call with hard-coded/config?
        '''
        url = f"{self.config['API']['TELAPI']}cmd=getTelnr&instr={self.instr.upper()}"
        data = self.get_api_data(url, getOne=True)
        self.telnr = int(data['TelNr'])
        if self.telnr not in [1, 2]:
            self.log_error('TELNR_VALUE_ERROR', telNr)
            return False
        return True


    def write_lev0_fits_file(self):

        #build outfile path and save as class var for reference later
        koaid = self.get_keyword('KOAID')
        self.outfile = f"{self.dirs['lev0']}/{koaid}"

        #write out new fits file with altered header
        try:
            self.fits_hdu.writeto(self.outfile)
            log.info('write_lev0_fits_file: output file is ' + self.outfile)
        except:
            try:
                self.fits_hdu.writeto(self.outfile, output_verify='ignore')
                log.info('write_lev0_fits_file: Forced to write FITS using output_verify="ignore". May want to inspect:' + self.outfile)                
            except Exception as e:
                self.log_error('WRITE_FITS_ERROR', str(e))
                if os.path.isfile(self.outfile):
                    os.remove(self.outfile)
                return False

        return True


    def make_jpg(self):
        '''
        Make the jpg(s) for current fits file
        '''

        # Find fits file in lev0 dir to convert based on koaid
        koaid = self.fits_hdr.get('KOAID')
        fits_filepath = ''
        for root, dirs, files in os.walk(self.dirs['lev0']):
            if koaid in files:
                fits_filepath = f'{root}/{koaid}'
        if not fits_filepath:
            self.log_warn('MAKE_JPG_FITS_ERROR', koaid)
            return False
        outdir = os.path.dirname(fits_filepath)

        #call instrument specific create_jpg function
        try:
            log.info(f'make_jpg: Creating jpg from: {fits_filepath}')
            self.create_jpg_from_fits(fits_filepath, outdir)
        except Exception as e:
            self.log_warn('MAKE_JPG_ERROR', traceback.format_exc())
            return False

        return True


    def create_jpg_from_fits(self, fits_filepath, outdir):
        '''
        Basic convert fits primary data to jpg.  Instrument subclasses can override this function.
        '''

        #get image data
        hdu = fits.open(fits_filepath, ignore_missing_end=True)
        data = hdu[0].data
        hdr  = hdu[0].header

        #form filepaths
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        jpg_filepath = f'{outdir}/{basename}.jpg'

        #create jpg
        interval = ZScaleInterval()
        vmin, vmax = interval.get_limits(data)
        norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
        dpi = 100
        width_inches  = hdr['NAXIS1'] / dpi
        height_inches = hdr['NAXIS2'] / dpi
        fig = plt.figure(figsize=(width_inches, height_inches), frameon=False, dpi=dpi)
        ax = fig.add_axes([0, 0, 1, 1]) #this forces no border padding
        plt.axis('off')
        plt.imshow(data, cmap='gray', origin='lower', norm=norm)
        plt.savefig(jpg_filepath, quality=92)
        plt.close()


    def get_semid(self):

        semester = self.get_keyword('SEMESTER')
        progid   = self.get_keyword('PROGID')

        if (semester == None or progid == None): 
            return None

        semid = semester + '_' + progid
        return semid


    def set_frameno(self):
        """
        Adds FRAMENO keyword to header if it doesn't exist
        """

        # log.info('set_frameno: setting FRAMNO keyword value from FRAMENUM')

        #skip if it exists
        if self.get_keyword('FRAMENO', False) != None: return True

        #get value
        #NOTE: If FRAMENO doesn't exist, derive from DATAFILE
        frameno = self.get_keyword('FRAMENUM')
        if (frameno == None): 

            datafile = self.get_keyword('DATAFILE')
            if (datafile == None): 
                self.log_warn("SET_FRAMENO_ERROR")
                return False

            frameno = datafile.replace('.fits', '')
            num = frameno.rfind('_') + 1
            frameno = frameno[num:]
            frameno = int(frameno)

            self.set_keyword('FRAMENUM', frameno, 'KOA: Image frame number (derived from filename)')

        #update
        self.set_keyword('FRAMENO', frameno, 'KOA: Image frame number')
        return True


    def is_science(self):
        '''
        Returns true if header indicates science data was taken.
        '''

        koaimtyp = self.get_keyword('KOAIMTYP')
        if koaimtyp == 'object' : return True
        else                    : return False


    def run_drp(self):
        '''
        This will be overwritten by method in instrument specific module.
        For those instruments without a DRP, just note that in the log.
        '''

        log.info('run_drp: no DRP defined for {}'.format(self.instr))
        return True

    def run_psfr(self):
        '''
        This will be overwritten by method in instrument specific module.
        For those instruments without PSFR, just note that in the log.
        '''

        log.info('run_psfr: no PSFR defined for {}'.format(self.instr))
        return True

    def set_numccds(self):
        try:
            panelist = self.get_keyword('PANELIST')
        except:
            self.set_keyword('NUMCCDS',0,'KOA: Number of CCDs')
            return True
        if panelist == '0':
            numccds = 0
        #mosaic data
        elif panelist == 'PANE':
            pane = self.get_keyword('PANE')
            plist = pane.split(',')
            dx = float(plist[2])
            if dx < 8192:
                numccds = 4
            elif dx < 6144:
                numccds = 3
            elif dx < 4096:
                numccds = 2
            elif dx < 2048:
                numccds = 1
            if 'LRISBLUE' in self.get_keyword('INSTRUME'):
                numccds = 1
                amplist = (self.get_keyword('AMPLIST')).split(',')
                if float(amplist[1]) > 2:
                    numccds = 2
        else:
            plist = panelist.split(',')
            numccds = 0
            for i,val in enumerate(plist):
                kywrd = f'PANE{str(val)}'
                pane = self.get_keyword(kywrd)
                plist2 = pane.split(',')
                dx = float(plist2[2])
                if dx < 2048:
                    numccds += 1
                else:
                    if dx < 4096:
                        numccds += 2
                    else:
                        numccds += 3

        self.set_keyword('NUMCCDS',numccds,'KOA: Number of CCDs')
        return True


    def dqa_loc(self, delete=0):
        '''
        Creates or deletes the dqa.LOC file.
        This file is needed for the PSF/TRS process.
        '''

        dqaLoc = f"{self.dirs['lev0']}/dqa.LOC"

        if delete == 0:
            if not os.path.isfile(dqaLoc):
                log.info(f'dqa_loc: creating {dqaLoc}')
                open(dqaLoc, 'w').close()
        elif delete == 1:
            if os.path.isfile(dqaLoc):
                log.info(f'dqa_loc: removing {dqaLoc}')
                os.remove(dqaLoc)
        else:
            log.info('dqa_loc: invalid input parameter')

        return True

