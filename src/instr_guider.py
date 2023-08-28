'''
This is the class to handle all the GUIDER specific attributes
'''

import instrument
import datetime as dt
import numpy as np
from astropy.io import fits
import os
import re
import matplotlib as mpl
import matplotlib.pyplot as plt
import math
from skimage import exposure
import traceback
import glob
from pathlib import Path
import shutil

import logging
log = logging.getLogger('koa_dep')


class Guider(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):

        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        #self.keymap['UTC'] = 'UT'
        self.rtui = False

    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',       'crit': True},
            {'name':'set_ut',          'crit': True},
            {'name':'set_ofName',      'crit': True},
            {'name':'set_koaimtyp',    'crit': True},
            {'name':'set_frameno',     'crit': True},
            {'name':'set_semester',    'crit': True},
            {'name':'set_prog_info',   'crit': True},
            {'name':'set_propint',     'crit': True},
            {'name':'set_elaptime',    'crit': False},
            {'name':'set_datlevel',    'crit': False, 'args': {'level':0}},
            {'name':'set_filter',      'crit': False},
            {'name':'set_wavelengths', 'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
            {'name':'set_dqa_vers',    'crit': False},
            {'name':'set_dqa_date',    'crit': False},
        ]
        return self.run_functions(funcs)

    def get_prefix(self):
        #instr = self.get_instr()
        instr = "GR"
        return instr

    def make_koaid(self):
        koaid = super().make_koaid()
        if koaid:
            camname = self.get_keyword('CAMNAME', default=None)
            if camname == None:
                koaid = ''
            else:
                koaid += f'_{camname}'
        return koaid

    def set_instr(self):
       instr = self.get_keyword('INSTRUME', default=None)
       if instr == None:
           currinst = self.get_keyword('CURRINST', default=None)
           if currinst == None:
               return False
           self.set_keyword('INSTRUME', currinst, 'KOA: Instrument Name')
       return True

    def create_jpg_from_fits(self, fits_filepath, outdir):
        '''
        Basic convert fits primary data to jpg.  Instrument subclasses can override this function.
        '''

        # Check to see if one exists in the original directory.  If not, create it.
        jpg = self.status['ofname'].replace('.fits', '.jpg')
        if os.path.isfile(jpg):
            log.info(f'Copying {jpg}')
            outfile = f"{self.dirs['lev0']}/{self.koaid}.jpg"
            shutil.copy(jpg, outfile)
        else:
            super().create_jpg_from_fits(fits_filepath, outdir)

    def set_telnr(self):
        """
        Gets telescope number for instrument via API
        """
        temp = self.get_keyword('TELESCOP').split(' ')[-1]
        if temp not in ['I', 'II']:
            self.log_error('TELNR_VALUE_ERROR', self.telNr)
            return False
        else:
            if temp == 'I':
                self.telnr = 1
            elif temp == 'II':
                self.telnr = 2;
        return True

    def set_ofName(self):
        """
        Sets OFNAME keyword and db value
        """
        of_name = os.path.basename(self.status['ofname'])
        ofname_keyword = self.get_keyword('OFNAME')
        if not ofname_keyword:
            log.info('Add keyword OFNAME')
            self.set_keyword('OFNAME', of_name, 'KOA: Original file name')
        return True

    def convert_to_start_end(self, utdate, start, duration):
        '''
        Converts UTDate, startTime, and duration into start/end times with
        format HH:MM and returns both.
        '''
        # create datetime with StartTime, add any missing 0's
        split = start.split(':')
        start = f'{split[0].zfill(2)}:{split[1].zfill(2)}:{split[2].zfill(2)}'
        thisdate = dt.datetime.strptime(f"{utdate} {start}", '%Y-%m-%d %H:%M:%S')
        startTime = thisdate.strftime('%H:%M')
        # Add the Duration
        split = duration.split(':')
        thisdate = thisdate + dt.timedelta(hours=int(split[0]), minutes=int(split[1]), seconds=int(split[2]))
        endTime = thisdate.strftime('%H:%M')
        return startTime,endTime

    def get_schedule_data(self, instr):
        '''
           Queries the schedule API to return ToO, twilight, and classical programs.
           Combines the results into a single list of dictionaries.
        '''
        api = self.config['API']['TELAPI']
        too = self.get_api_data(f'{api}cmd=getToORequest&date={self.hstdate}')
        sched = []
        for entry in too:
            if entry['Instrument'] != instr or entry['StartTimeActual'] == None:
                continue
            proj = {}
            proj['Type'] = 'ToO'
            proj['Date'] = entry['ObsDate']
            for key in ['TelNr','Instrument','ProjCode']:
                proj[key] = entry[key]
            proj['StartTime'],proj['EndTime'] = \
                self.convert_to_start_end(self.utdate, entry['StartTimeActual'], entry['DurationActual'])
            sched.append(proj)

        url = api.replace('telSchedule','twilightApi')
        twilight = self.get_api_data(f'{url}cmd=twilight_select&utdate={self.utdate}')
        for entry in twilight:
            if instr not in entry['Instr']:
                continue
            proj = {}
            proj['Type'] = 'Twilight'
            proj['Date'] = self.hstdate
            for key in ['TelNr','Instr','ProjCode']:
                proj[key] = entry[key]
            proj['StartTime'],proj['EndTime'] = \
                self.convert_to_start_end(self.utdate, entry['StartTime'], entry['Duration'])
            sched.append(proj)

        classical = self.get_api_data(f'{api}cmd=getSchedule&date={self.hstdate}&instr={instr}')
        for entry in classical:
            proj = {}
            proj['Type'] = 'Classical'
            for key in ['Date','TelNr','Instrument','ProjCode','StartTime','EndTime']:
                proj[key] = entry[key]
            sched.append(proj)

        return sched

    def get_progid_from_schedule(self):
        """
        Try to set PROGID from the information in the telescope schedule.
        This overrides the version in instrument.py.
        """

        #requires UTC value
        ut = self.get_keyword('UTC')
        if not ut: return 'NONE'
        ut = ut.split(':')
        ut = int(ut[0]) + (int(ut[1])/60.0)

        instr_name = self.get_keyword('CURRINST', default=None)
        if instr_name == "LRISADC":
            instr_name = "LRIS"
        
        #SSC and PCS do not have ProgIDs in Schedule yet
        if instr_name == "SSC" or instr_name == "PCS":
            return 'NONE'

        data = self.get_schedule_data(instr_name)
        if data:
            if isinstance(data, dict):
                data = [data]
            if len(data) == 1:
                log.warning(f"Assigning PROGID by only scheduled entry: {data[0]['ProjCode']}")
                return data[0]['ProjCode']
            for num, entry in enumerate(data):
                start = entry['StartTime'].split(':')
                start = int(start[0]) + (int(start[1])/60.0)
                end = entry['EndTime'].split(':')
                end = int(end[0]) + (int(end[1])/60.0)
                if ut >= start and ut <= end:
                    log.warning(f"Assigning PROGID by schedule UTC: {entry['ProjCode']}")
                    return entry['ProjCode']
                if entry['Type'] == 'Classical':
                    if num == 0 and ut < start:
                        log.warning(f"Assigning PROGID by first scheduled entry: {entry['ProjCode']}")
                        return entry['ProjCode']
                    if num == len(data)-1 and ut > end:
                        log.warning(f"Assigning PROGID by last scheduled entry: {entry['ProjCode']}")
                        return entry['ProjCode']
        return 'NONE'

    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm
        Calls get_koaimtyp for algorithm
        '''

        koaimtyp = self.get_koaimtyp()
        
        ttime = self.get_keyword('TTIME')
        if ttime == 0:
            koaimtyp = 'bias'
        else:
            koaimtyp = 'object'

        #update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        
        return True

    def get_koaimtyp(self):
        '''
        Sets koaimtyp based on keyword values
        '''
        koaimtyp = 'undefined'
        try:
            camera = self.get_keyword('CAMERA').lower()
        except:
            camera = ''
        if camera == 'fpc':
            koaimtyp = 'fpc'
        elif self.get_keyword('XPOSURE') == 0.0:
            koaimtyp = 'bias'
        elif self.get_keyword('IMTYPE'):
            koaimtyp = self.get_keyword('IMTYPE').lower()
        return koaimtyp

    # modified from instrument.py
    def set_frameno(self):
        """
        Adds FRAMENO keyword to header if it doesn't exist
        """

        # skip if it exists
        if self.get_keyword('FRAMENO', False) != None: return True

        # derive FRAMENO value from the original filename if it doesn't exist
        frameno = self.get_keyword('FRAMENO')
        if (frameno == None):

            ofname = os.path.basename(self.filepath)
            if (ofname == None):
                self.log_warn("SET_FRAMENO_ERROR")
                return False

            frameno = ofname.replace('.fits', '')
            num = frameno.rfind('_') + 1
            frameno = frameno[num:]
            frameno = int(frameno)

            self.set_keyword('FRAMENO', frameno, 'KOA: Image frame number (derived from filename)')

        # update existing FRAMENO
        self.set_keyword('FRAMENO', frameno, 'KOA: Image frame number')
        return True

    # from instr_lris.py
    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword
        '''

        #skip it it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        elaptime = 'null'

        #get necessary keywords
        ttime  = self.get_keyword('TTIME')
        if ttime != None:
            log.info('set_elaptime: determining ELAPTIME from TTIME')
            elaptime = round(ttime)

        if elaptime == 'null':
            log.warn('set_elaptime: Could not set ELAPTIME')

        #update val
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')

        return True


    def set_filter(self):
        '''
        If FILTER keyword doesn't exist, create from FILTER0 and FILTER1
        '''

        if self.get_keyword('FILTER', False) != None: return True

        filter0 = self.get_keyword('FILTER0', default='')
        filter1 = self.get_keyword('FILTER1', default='')
        if filter0 == '' and filter1 == '':
            filter = 'blank'
        else:
            filter = '+'.join(filter(None,(filter0, filter1)))

        #update keyword
        self.set_keyword('FILTER', filter, 'KOA: set from FILTER0 and FILTER1')
        return True


    def set_wavelengths(self):
        '''
        Sets WAVEBLUE, CNTR, RED (in microns, u) based on FILTER value
        '''
        filters = {}
        filters['ACAM']       = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['ACAMA']      = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['DEIMOS']     = {'blue':'null', 'cntr':'null', 'red':'null'} # tbd
        filters['ESI']        = {'blue':'null', 'cntr':'null', 'red':'null'}
        filters['HIRESSLIT']  = {'blue':3.600,  'cntr':6.800,  'red':10.000}
        filters['BG38']       = {'blue':3.350,  'cntr':4.700,  'red':6.050}
        filters['KCWIA']      = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['KPF']        = {'blue':0.950,  'cntr':1.075,  'red':1.200}
        filters['LRISOFFSET'] = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['V']          = {'blue':5.000,  'cntr':6.000,  'red':7.000}
        filters['LRISSLIT']   = {'blue':3.800,  'cntr':6.400,  'red':7.000}  # same as LRISOFFSET
        filters['MOSFIRE']    = {'blue':'null', 'cntr':'null', 'red':'null'} # tbd
        filters['NIRESA']     = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['NIRESSPLIT']    = {'blue':1.9500, 'cntr':2.1225, 'red':2.2950} #  K'
        filters['NIRSPECM']   = {'blue':3.800,  'cntr':6.400,  'red':7.000}
        filters['RG780']      = {'blue':7.800,  'cntr':'null', 'red':'null'} # tbd
        filters['NSCAM']      = {'blue':'null', 'cntr':'null', 'red':'null'} # tbd

        # replace open with <blank> (use actual filter)

        filter = self.get_keyword('FILTER', default='')

        waveblue = wavecntr = wavered = 'null'
        for filt, waves in filters.items():
            if filt in filter.upper():
                waveblue = waves['blue']
                wavecntr = waves['cntr']
                wavered = waves['red']
                break

        self.set_keyword('WAVEBLUE', waveblue, 'KOA: Approximate blue end wavelength (u)')
        self.set_keyword('WAVECNTR', wavecntr, 'KOA: Approximate central wavelength (u)')
        self.set_keyword('WAVERED', wavered, 'KOA: Approximate red end wavelength (u)')

        return True

