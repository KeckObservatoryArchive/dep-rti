'''
This is the class to handle all the ESI specific attributes.
'''

import instrument
import datetime as dt
from common import *
import numpy as np

import logging
log = logging.getLogger('koa_dep')


class Esi(instrument.Instrument):

    def __init__(self, instr, filepath, config, db, reprocess, tpx):

        super().__init__(instr, filepath, config, db, reprocess, tpx)

        # Set any unique keyword index values here
        self.keymap['UTC']      = 'UT'        
        self.keymap['OFNAME']   = 'OUTFILE'

        # Other vars that subclass can overwrite
        self.keyskips   = ['PMFM', 'RECNO', 'CHECKSUM', 'DATASUM']


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        ok = True
        if ok: ok = super().run_dqa()
        if ok: ok = self.set_filter()
        self.get_obsmode(update=True)
        if ok: ok = self.set_camera()
        if ok: ok = self.set_koaimtyp()
        if ok: ok = self.set_ut()
        if ok: ok = self.set_frameno()
        if ok: ok = self.set_ofName()
        if ok: ok = self.set_semester()
        if ok: ok = self.set_prog_info()
        if ok: ok = self.set_propint()
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.set_image_stats_keywords()
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_npixsat(65535)

        if ok: ok = self.set_wavelengths()
        if ok: ok = self.set_slit_dims()
        if ok: ok = self.set_spatscal()
        if ok: ok = self.set_dispscal()
        if ok: ok = self.set_specres()
        if ok: ok = self.set_dqa_vers()
        if ok: ok = self.set_dqa_date()
        return ok


    @staticmethod
    def get_dir_list():
        '''
        Function to generate the paths to all the ESI accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata70'
        for i in range(8):
            if i != 5:
                path2 = path + str(i) + '/esi'
                for j in range(1,21):
                    path3 = path2 + str(j)
                    dirs.append(path3)
                path3 = path2 + 'eng'
                dirs.append(path3)
        return dirs


    def get_prefix(self):

        instr = self.get_instr()
        if instr == 'esi': prefix = 'ES'
        else             : prefix = ''
        return prefix


    def set_koaimtyp(self):
        """
        Uses get_koaimtyp to set KOAIMTYP
        """

        #log.info('set_koaimtyp: setting KOAIMTYP keyword value')

        koaimtyp = self.get_koaimtyp()

        # Warn if undefined
        if koaimtyp == 'undefined':
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')

        # Update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')

        return True

    def set_ofName(self):
        '''
        Sets OFNAME keyword from OUTFILE and FRAMENO
        '''

        outfile = self.get_keyword('OUTFILE', False)
        frameno = self.get_keyword('FRAMENO', False)
        if outfile == None or frameno == None:
            log.warning('set_ofName: Could not determine OFNAME')
            return False
    
        frameno = str(frameno).zfill(4)
        ofName = ''.join((outfile, frameno, '.fits'))
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')

        return True


    def get_obsmode(self, update=False):
        """
        Determines spectrograph dispersion mode (low, high, image)
        """

        obsmode = self.get_keyword('OBSMODE')
        if obsmode == None:
            imfltnam = self.get_keyword('IMFLTNAM', default='').lower()
            ldfltnam = self.get_keyword('LDFLTNAM', default='').lower()
            prismnam = self.get_keyword('PRISMNAM', default='').lower()

            if   imfltnam == 'out' and ldfltnam == 'in'  and prismnam == 'in' : obsmode = 'low'
            elif imfltnam == 'out' and ldfltnam == 'out' and prismnam == 'in' : obsmode = 'high'
            elif imfltnam == 'in'  and ldfltnam == 'out' and prismnam == 'out': obsmode = 'image'

            if update: self.set_keyword('OBSMODE', obsmode, 'KOA: Observation mode')

        return obsmode


    def set_camera(self):
        '''
        Set CAMERA keyword to constanct ESI
        '''
        camera = 'ESI'
        self.set_keyword('CAMERA', camera, 'KOA: Instrument camera')
        return True


    def get_koaimtyp(self):
        """
        Determine image type based on instrument keyword configuration
        """

        # Default KOAIMTYP value
        koaimtyp = 'undefined'

        # Check OBSTYPE first
        obstype = self.get_keyword('OBSTYPE', default='').lower()

        if obstype == 'bias': return 'bias'
        if obstype == 'dark': return 'dark'

        slmsknam = self.get_keyword('SLMSKNAM', default='').lower()
        hatchpos = self.get_keyword('HATCHPOS', default='').lower()
        lampqtz1 = self.get_keyword('LAMPQTZ1', default='').lower()
        lampar1 = self.get_keyword('LAMPAR1', default='').lower()
        lampcu1 = self.get_keyword('LAMPCU1', default='').lower()
        lampne1 = self.get_keyword('LAMPNE1', default='').lower()
        lampne2 = self.get_keyword('LAMPNE2', default='').lower()
        prismnam = self.get_keyword('PRISMNAM', default='').lower()
        imfltnam = self.get_keyword('IMFLTNAM', default='').lower()
        axestat = self.get_keyword('AXESTAT', default='').lower()
        domestat = self.get_keyword('DOMESTAT', default='').lower()
        el = self.get_keyword('EL')
        dwfilnam = self.get_keyword('DWFILNAM', default='').lower()
        ldfltnam = self.get_keyword('LDFLTNAM', default='').lower()

        # Hatch
        hatchOpen = 1
        if hatchpos == 'closed': hatchOpen = 0

        # Is flat lamp on?
        flat = 0
        if lampqtz1 == 'on': flat = 1

        # Is telescope pointed at flat screen?
        flatPos = 0
        if el != None and el >= 44.0 and el <= 46.01: flatPos = 1

        # Is an arc lamp on?
        arc = 0
        if lampar1 == 'on' or lampcu1 == 'on' or lampne1 == 'on' or lampne2 == 'on':
            arc = 1

        # Dome/Axes tracking
        axeTracking = domeTracking = 0
        if axestat == 'tracking': axeTracking = 1
        if domestat == 'tracking': domeTracking = 1

        # This is a trace or focus
        if 'hole' in slmsknam:
            if not hatchOpen:
                if flat and not arc and prismnam == 'in' and imfltnam == 'out': 
                    return 'trace'
                if flat and not arc and prismnam != 'in' and imfltnam != 'out': 
                    return 'focus'
                if not flat and arc and prismnam == 'in' and imfltnam == 'out': 
                    return 'focus'
            else:
                if prismnam == 'in' and imfltnam == 'out':
                    if obstype == 'dmflat' and not domeTracking and flatPos: 
                        return 'trace'
                    if not axeTracking and not domeTracking and flatPos: 
                        return 'trace'
                    if obstype == 'dmflat' and not axeTracking and not domeTracking and flatPos: 
                        return 'trace'
                    if obstype == 'dmflat' and not axeTracking and flatPos: 
                        return 'trace'
                else:
                    if obstype == 'dmflat' and not domeTracking and flatPos: 
                        return 'focus'
                    if not axeTracking and not domeTracking and flatPos: 
                        return 'focus'
                    if obstype == 'dmflat' and not axeTracking and not domeTracking and flatPos: 
                        return 'focus'
                    if obstype == 'dmflat' and not axeTracking and flatPos: 
                        return 'focus'
            if prismnam == 'out' and imfltnam == 'in' and ldfltnam == 'out': 
                return 'focus'
            if prismnam == 'in' and imfltnam == 'out' and dwfilnam == 'clear_s': 
                return 'focus'
        #if not hole in slmsknam
        else:
            #if hatch closed
            if not hatchOpen:
                if flat and not arc: 
                    return 'flatlamp'
                if not flat and arc and prismnam == 'in' and imfltnam == 'out': 
                    return 'arclamp'
            #if hatch open
            else:
                if obstype == 'dmflat' and not domeTracking and flatPos: 
                    return 'flatlamp'
                if not axeTracking and not domeTracking and flatPos: 
                    return 'flatlamp'
                if obstype == 'dmflat' and not axeTracking and not domeTracking: 
                    return 'flatlamp'
                if obstype == 'dmflat' and not axeTracking and flatPos: 
                    return 'flatlamp'
                if not flat and not arc: 
                    return 'object'

        return 'undefined'


    def set_filter(self):
        '''
        Add filter which is copy of DWFILNAM
        '''
        dwfilnam  = self.get_keyword('dwfilnam')
        if dwfilnam:
            self.set_keyword('FILTER', dwfilnam, 'KOA: Filter name copied from DWFILNAM.')
        return True


    def set_wavelengths(self):
        '''
        Adds wavelength keywords.
        '''

        # log.info('set_wavelengths: setting wavelength keyword values')

        # Default null values
        wavered = wavecntr = waveblue = 'null'

        obsmode  = self.get_keyword('OBSMODE')

        #imaging:
        if obsmode == 'image':
            esifilter = self.get_keyword('DWFILNAM')
            if esifilter == 'B':
                wavered  = 5400
                wavecntr = 4400
                waveblue = 3700
            elif esifilter == 'V':
                wavered  = 6450  
                wavecntr = 5200
                waveblue = 4900
            elif esifilter == 'R':
                wavered  = 7400  
                wavecntr = 6500
                waveblue = 6000
            elif esifilter == 'I':
                wavered  = 9000
                wavecntr = 8000
                waveblue = 7000

        #spec:
        elif obsmode in ('low', 'high'):
            wavered = 10900
            wavecntr =  7400
            waveblue =  3900

        self.set_keyword('WAVERED' , wavered, 'KOA: Red end wavelength')
        self.set_keyword('WAVECNTR', wavecntr, 'KOA: Center wavelength')
        self.set_keyword('WAVEBLUE', waveblue, 'KOA: Blue end wavelength')

        return True


    def set_specres(self):
        '''
        Adds nominal spectral resolution keyword
        '''

        # log.info('set_specres: setting SPECRES keyword values')

        specres = 'null'
        obsmode   = self.get_keyword("OBSMODE")
        if obsmode in ('low', 'high'):
            #spectral resolution R found over all wavelengths and dispersions between orders 6-15
            #
            #           wavelength           0.1542[arcsec/pixel] * wavelength[angstroms]
            # R    =   -----------     =    ---------------------------------------------
            #         deltawavelength       slitwidth[arcsec] * dispersion[angstroms/pixel]
            # 
            #           MEAN(0.1542*wavelength/dispersion)         4125.406
            # R    =    -----------------------------------   =   -----------
            #                       slitwidth                      slitwidth
            #
            #from echellette table https://www.keck.hawaii.edu/realpublic/inst/esi/Sensitivities.html
            try:
                slitwidt = self.get_keyword('SLITWIDT')
                specres = 4125.406 / slitwidt
                specres = np.round(specres,-1)
            except:
                pass
        self.set_keyword('SPECRES' , specres,  'KOA: Nominal spectral resolution')
        return True


    def set_dispscal(self):
        '''
        Adds CCD pixel scale, dispersion (arcsec/pixel) keyword to header.
        '''
        dispscal = 0.1542 #arcsec/pixel
        self.set_keyword('DISPSCAL' , dispscal, 'KOA: CCD pixel scale, dispersion')
        return True


    def set_spatscal(self):
        '''
        Adds spatial scale keyword to header.
        '''
        spatscal = 0.1542 #arsec/pixel
        self.set_keyword('SPATSCAL' , spatscal, 'KOA: CCD pixel scale, spatial')
        return True


    def set_slit_dims(self):
        '''
        Adds slit length and width keywords to header.
        '''

        obsmode = self.get_obsmode()

        slitlen = 'null'
        slitwidt = 'null'

        #values for 'spec' only
        if obsmode in ('low', 'high'):

            slmsknam = self.get_keyword('SLMSKNAM', default='').lower()

            #IFU (5 slices that are 1.13 arcseconds wide)
            if slmsknam == 'ifu':
                slitwidt = 1.13
                slitlen  = 4.0 

            #standard
            else:
                if   obsmode == 'low' : slitlen = 8*60 #8 arcminutes = 480 arcseconds
                elif obsmode == 'high': slitlen = 20   #20 arcseconds

                if 'multiholes' in slmsknam:
                    slitwidt = 0.5
                elif '_' in slmsknam:
                    parts = slmsknam.split('_')
                    try:
                        slitwidt = float(parts[0])
                    except:
                        try:
                            slitwidt = float(parts[1])
                        except:
                            slitwidt = 'null'

        self.set_keyword('SLITWIDT' , slitwidt, 'KOA: Slit width projected on sky')
        self.set_keyword('SLITLEN'  , slitlen,  'KOA: Slit length projected on sky')

        return True
