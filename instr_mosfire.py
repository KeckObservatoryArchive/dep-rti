'''
This is the class to handle all the MOSFIRE specific attributes
MOSFIRE specific DR techniques can be added to it in the future

12/14/2017 M. Brown - Created initial file
'''

import instrument
import datetime as dt
from common import *
import numpy as np


class Mosfire(instrument.Instrument):

    def __init__(self, instr, utDate, rootdir, log=None):

        # Call the parent init to get all the shared variables
        super().__init__(instr, utDate, rootdir, log)


        # Set any unique keyword index values here
        self.keymap['OFNAME']       = 'DATAFILE'        
        self.keymap['FRAMENO']      = 'FRAMENUM'


        # Other vars that subclass can overwrite
        self.endTime = '19:00:00'   # 24 hour period start/end time (UT)
        self.keyskips   = ['B\d+STAT', 'B\d+POS']


        # Generate the paths to the NIRES datadisk accounts
        self.sdataList = self.get_dir_list()



    def run_dqa(self, progData):
        '''
        Run all DQA checks unique to this instrument.
        '''

        #todo: check that all of these do not need a subclass version if base class func was used.
        ok = True
        if ok: ok = self.set_elaptime()
        if ok: ok = self.set_koaimtyp()
        if ok: ok = self.set_ut()
        if ok: ok = self.set_frameno()
        if ok: ok = self.set_ofName()
        if ok: ok = self.set_semester()
        if ok: ok = self.set_prog_info(progData)
        if ok: ok = self.set_propint(progData)
        if ok: ok = self.set_wavelengths()
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.set_image_stats_keywords()
        if ok: ok = self.set_npixsat()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_dqa_date()
        if ok: ok = self.set_dqa_vers()
        return ok


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
        #todo: make this a common instrument.py function for all instruments (small differences)

        # self.log.info('set_elaptime: determining ELAPTIME from ITIME/COADDS')

        #skip if it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        #get necessary keywords
        itime  = self.get_keyword('TRUITIME')
        coadds = self.get_keyword('COADDS')
        if (itime == None or coadds == None):
            self.log.error('set_elaptime: TRUITIME and COADDS values needed to set ELAPTIME')
            return False

        #update val
        elaptime = round(itime * coadds, 4)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        return True


    def set_koaimtyp(self):
        """
        Determine image type based on instrument keyword configuration
        """

        # self.log.info('set_koaimtyp: setting KOAIMTYP keyword value')

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
            self.log.info('set_koaimtyp: Could not determine KOAIMTYP value')

        # Update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        return True


    def set_wavelengths(self):
        """
        Adds wavelength keywords.
        # https://www2.keck.hawaii.edu/inst/mosfire/filters.html
        """

        # self.log.info('set_wavelengths: setting wavelength keyword values')

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

