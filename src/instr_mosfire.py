'''
This is the class to handle all the MOSFIRE specific attributes
'''

import instrument
from common import *
import numpy as np

import logging
main_logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class Mosfire(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        self.keymap['OFNAME']       = 'DATAFILE'        
        self.keymap['FRAMENO']      = 'FRAMENUM'

        # Other vars that subclass can overwrite
        self.keyskips   = ['B\d+STAT', 'B\d+POS']


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',        'crit': True},
            {'name':'set_ut',           'crit': True},
            {'name':'set_frameno',      'crit': True},
            {'name':'set_ofName',       'crit': True},
            {'name':'set_semester',     'crit': True},
            {'name':'set_prog_info',    'crit': True},
            {'name':'set_propint',      'crit': True},
            {'name':'set_koaimtyp',     'crit': True},
            {'name':'set_elaptime',     'crit': False},
            {'name':'set_wavelengths',  'crit': False},
            {'name':'set_weather',      'crit': False},
            {'name':'set_datlevel',     'crit': False,  'args': {'level':0}},
            {'name':'set_image_stats',  'crit': False},
            {'name':'set_npixsat',      'crit': False},
            {'name':'set_oa',           'crit': False},
            {'name':'set_dqa_date',     'crit': False},
            {'name':'set_dqa_vers',     'crit': False},
        ]
        return self.run_functions(funcs)


    @staticmethod
    def get_dir_list():
        '''
        Function to generate the paths to all the MOSFIRE accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata1300'
        path2 = path + '/mosfire'
        dirs.append(path2)
        for i in range(1,10):
            path2 = path + '/mosfire' + str(i)
            dirs.append(path2)
        path2 = path + '/moseng'
        dirs.append(path2)
        return dirs


    def get_prefix(self):

        instr = self.get_instr()
        if instr == 'mosfire': prefix = 'MF'
        else                 : prefix = ''
        return prefix


    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword.
        '''
        #skip if it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        #get necessary keywords
        itime  = self.get_keyword('TRUITIME')
        coadds = self.get_keyword('COADDS')
        if (itime == None or coadds == None):
            self.log_warn('SET_ELAPTIME_ERROR')
            return False

        #update val
        elaptime = round(itime * coadds, 4)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        return True


    def set_koaimtyp(self):
        """
        Determine image type based on instrument keyword configuration
        """

        # Default KOAIMTYP value
        koaimtyp = 'undefined'

        # Telescope and dome keyword values
        el = self.get_keyword('EL')
        if not isinstance(el, float): el = 0.0
        az = self.get_keyword('AZ')
        if not isinstance(az, float): az = 0.0
        domeposn = self.get_keyword('DOMEPOSN')
        if not isinstance(domeposn, float): domeposn = 0.0
        domestat = self.get_keyword('DOMESTAT')
        axestat = self.get_keyword('AXESTAT')

        # MOSFIRE keyword values
        obsmode = self.get_keyword('OBSMODE', default='')
        maskname = self.get_keyword('MASKNAME')
        mdcmech = self.get_keyword('MDCMECH')
        mdcstat = self.get_keyword('MDCSTAT')
        mdcname = self.get_keyword('MDCNAME')

        # Dome lamp keyword values
        flatspec = self.get_keyword('FLATSPEC')
        flimagin = self.get_keyword('FLIMAGIN')
        flspectr = self.get_keyword('FLSPECTR')
        flatOn = 0
        if flatspec == 1 or flimagin == 'on' or flspectr == 'on':
            flatOn = 1

        # Arc lamp keyword values
        pwstata7 = self.get_keyword('PWSTATA7')
        pwstata8 = self.get_keyword('PWSTATA8')
        power = 0
        if pwstata7 == 1 or pwstata8 == 1:
            power = 1

        # Is telescope in flatlamp position
        flatlampPos = 0
        azDiff = abs(domeposn - az)
        if (44.99 <= el <= 45.01) and (89.00 <= azDiff <= 91.00):
            flatlampPos = 1

        # Is the dust cover open
        dustCover = ''
        if  mdcmech == 'Dust Cover' and mdcstat == 'OK':
            dustCover = mdcname.lower()

        # Dark frame
        if 'dark' in obsmode.lower() and not power:
            koaimtyp = 'dark'
        else:
            # Setup for arclamp
            if dustCover == 'closed':
                if 'spectroscopy' in obsmode and power:
                    koaimtyp = 'arclamp'
            elif dustCover == 'open':
                # This is an object unless a flatlamp is on
                koaimtyp = 'object'
                if   flatOn     : koaimtyp = 'flatlamp'
                elif flatlampPos: koaimtyp = 'flatlampoff'

        # Still undefined? Use image statistics
        if koaimtyp == 'undefined':
            # Is the telescope in dome flat position?
            if flatlampPos:
                image = self.fits_hdu[0].data
                imageMean = np.mean(image)
                koaimtyp = 'flatlampoff'
                if (imageMean > 500):
                    koaimtyp = 'flatlamp'

        # Warn if undefined
        if koaimtyp == 'undefined':
            main_logger.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")

        # Update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        return True


    def set_wavelengths(self):
        """
        Adds wavelength keywords.
        # https://www2.keck.hawaii.edu/inst/mosfire/filters.html
        """
        # Filter lookup (filter: [central, fwhm])
        wave = {}
        wave['Y'] = [1.048, 0.152]
        wave['J'] = [1.253, 0.200]
        wave['H'] = [1.637, 0.341]
        wave['K'] = [2.162, 0.483]
        wave['Ks'] = [2.147, 0.314]
        wave['J2'] = [1.181, 0.129]
        wave['J3'] = [1.288, 0.122]
        wave['H1'] = [1.556, 0.165]
        wave['H2'] = [1.709, 0.167]

        # Default null values
        wavered = wavecntr = waveblue = 'null'

        filter = self.get_keyword('FILTER')

        if filter in wave.keys():
            fwhm = wave[filter][1] / 2.0
            wavecntr = wave[filter][0]
            waveblue = wavecntr - fwhm
            wavered = wavecntr + fwhm
            waveblue = float('%.3f' % waveblue)
            wavecntr = float('%.3f' % wavecntr)
            wavered = float('%.3f' % wavered)

        self.set_keyword('WAVERED' , wavered, 'KOA: Red end wavelength')
        self.set_keyword('WAVECNTR', wavecntr, 'KOA: Center wavelength')
        self.set_keyword('WAVEBLUE', waveblue, 'KOA: Blue end wavelength')

        return True


    def has_target_info(self):
        '''
        Does this fits have target info?
        If any header name is not PrimaryHDU or ImageHDU, then yes.
        '''
        has_target = False
        non_target_names = ['PrimaryHDU', 'ImageHDU']
        for ext in range(0, len(self.fits_hdu)):
            if not any(x in str(type(self.fits_hdu[ext])) for x in non_target_names):
                has_target = True
        return has_target


    def get_drp_destfile(self, koaid, srcfile):
        '''
        Returns the destination of the DRP file.  Uses the PypeIt version.
        '''
        return self.get_pypeit_drp_destfile(koaid, srcfile)


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Returns a list of files to archive for the DRP specific to MOSFIRE.
        '''
        return self.get_pypeit_drp_files_list(datadir, koaid, level)

