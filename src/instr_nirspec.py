'''
This is the class to handle all the NIRSPEC specific attributes
'''

import instrument
import datetime as dt
from common import *
from math import ceil

import logging
log = logging.getLogger('koa_dep')


class Nirspec(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer):

        super().__init__(instr, filepath, reprocess, transfer)

        #set any unique keyword index values here
        self.keymap['OFNAME'] = 'DATAFILE'
        self.keymap['FRAMENO'] = 'FRAMENUM'


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',        'crit': True},
            {'name':'set_dqa_date',     'crit': True},
            {'name':'set_dqa_vers',     'crit': True},
            {'name':'set_ut',           'crit': True},
            {'name':'set_elaptime',     'crit': True},
            {'name':'set_koaimtyp',     'crit': True},
            {'name':'set_frameno',      'crit': True},
            {'name':'set_ofName',       'crit': True},
            {'name':'set_semester',     'crit': True},
            {'name':'set_prog_info',    'crit': True},
            {'name':'set_propint',      'crit': True},
            {'name':'set_isao',         'crit': False},
            {'name':'set_dispers',      'crit': False},
            {'name':'set_slit_values',  'crit': False},
            {'name':'set_filter',       'crit': False},
            {'name':'set_wavelengths',  'crit': False},
            {'name':'set_weather',      'crit': False},
            {'name':'set_image_stats',  'crit': False},
            {'name':'set_gain_and_rn',  'crit': False},
            {'name':'set_npixsat',      'crit': False},
            {'name':'set_oa',           'crit': False},
            {'name':'set_datlevel',     'crit': False,  'args': {'level':0}},
        ]
        return self.run_functions(funcs)


    def get_dir_list(self):
        '''
        Function to generate the paths to all the NIRSPEC accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata60'
        for i in range(4):
            joinSeq = (path, str(i))
            path2 = ''.join(joinSeq)
            for j in range(1,10):
                joinSeq = (path2, '/nspec', str(j))
                path3 = ''.join(joinSeq)
                dirs.append(path3)
            joinSeq = (path2, '/nspeceng')
            path3 = ''.join(joinSeq)
            dirs.append(path3)
            joinSeq = (path2, 'nirspec')
            path3 = ''.join(joinSeq)
            dirs.append(path3)
        return dirs


    def get_prefix(self):

        # SCAM = NC, SPEC = NS
        instr = self.get_instr()
        if instr == 'nirspec' or instr == 'nirspao':
            try:
                camera = self.get_keyword('CAMERA')
                if camera == None:
                    camera = self.get_keyword('OUTDIR')
                camera = camera.lower()
            except KeyError:
                prefix = ''
            else:
                if 'scam' in camera:
                    prefix = 'NC'
                elif 'spec' in camera:
                    prefix = 'NS'
                else:
                    prefix = ''
        else:
            prefix = ''
        return prefix


    def set_instr(self):
        '''
        Overrides instrument.set_instr
        '''

        ok = False

        instrume = self.get_keyword('INSTRUME')
        if (instrume.strip() == 'NIRSPAO'): instrume = 'NIRSPEC'
 
        if (self.instr == instrume.strip()): ok = True

        return ok


    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword
        '''

        #skip it it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        #get necessary keywords
        itime  = self.get_keyword('TRUITIME')
        coadds = self.get_keyword('COADDS')
        if (itime == None or coadds == None):
            self.log_warn("SET_ELAPTIME_ERROR")
            return False

        #update val
        elaptime = round(itime * coadds, 5)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')

        return True


    def set_ofName(self):
        """
        Adds OFNAME keyword to header
        """

        #OFNAME was added as a native NIRSPEC keyword around 20190405
        if self.get_keyword('OFNAME', False) != None: return True

        #get value
        ofName = self.get_keyword('OFNAME')
        if (ofName == None):
            self.log_warn("SET_OFNAME_ERROR")
            return False

        #add *.fits to output if it does not exist (to fix old files)
        if (ofName.endswith('.fits') == False) : ofName += '.fits'

        #update
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')
        return True


    def set_koaimtyp(self):
        '''
        Fixes missing KOAIMTYP keyword.
        This is derived from OBSTYPE keyword.
        '''

        #get obstype value
        obstype = self.get_keyword('OBSTYPE')

        #map to KOAIMTYP value
        koaimtyp = 'undefined'
        validValsMap = {
            'object'  : 'object',
            'standard': 'object',   #NOTE: old val
            'telluric': 'object',
            'bias'    : 'bias',
            'dark'    : 'dark',
            'domeflat': 'domeflat',
            'domearc' : 'domearc',
            'arclamp' : 'arclamp',
            'flatlamp': 'flatlamp',
#            'astro'   : 'object',   #NOTE: old val
#            'star'    : 'object',   #NOTE: old val
#            'calib'   : 'undefined' #NOTE: old val
        }

        #first use OBSTYPE value
        if (obstype != None and obstype.lower() in validValsMap):
            koaimtyp = validValsMap[obstype.lower()]

        #use algorithm
        else:
            log.info('set_koaimtyp: setting KOAIMTYP keyword value from algorithm')

            calmpos = self.get_keyword('CALMPOS', default='').lower()
            calppos = self.get_keyword('CALPPOS', default='').lower()
            #calcpos doesn't exist in header
#            calcpos = self.get_keyword('CALCPOS', default='').lower()
            xenon = self.get_keyword('XENON', default='').lower()
            krypton = self.get_keyword('KRYPTON', default='').lower()
            argon = self.get_keyword('ARGON', default='').lower()
            neon = self.get_keyword('NEON', default='').lower()
#flat doesn't exist
#            flat = self.get_keyword('FLAT')
            flimagin = self.get_keyword('FLIMAGIN', default='').lower()
            flspectr = self.get_keyword('FLSPECTR', default='').lower()
            flat = 0
            if flimagin == 'on' or flspectr == 'on':
                flat = 1

            #arclamp
            if argon == 'on' or krypton == 'on' or neon == 'on' or xenon == 'on':
                if calmpos == 'in' and calppos == 'out':
                    koaimtyp = 'arclamp'
                else:
                    koaimtyp = 'undefined'

            #flats
            elif flat == 0 and calmpos == 'in':
                koaimtyp = 'flatlampoff'
            elif flat == 1 and calmpos == 'in' and calppos == 'out':
                koaimtyp = 'flatlamp'

            #darks
            elif int(self.get_keyword('ITIME')) == 0:
                koaimtyp = 'bias'

            #object
            elif calmpos == 'out' and calppos == 'out':
                koaimtyp = 'object'

            else:
                koaimtyp = 'undefined'

        #warn if undefined
        if (koaimtyp == 'undefined'):
            log.info('set_koaimtyp: Could not determine KOAIMTYP from OBSTYPE value')

        #update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        return True


    def set_filter(self):
        '''
        If FILTER keyword doesn't exist, create from SCIFILT1 and 2
        '''

        if self.get_keyword('FILTER', False) != None: return True

        log.info('set_filter: setting FILTER keyword from SCIFILT1/2')

        scifilt1 = self.get_keyword('SCIFILT1', default='')
        scifilt2 = self.get_keyword('SCIFILT2', default='')

        skip = ['thick', 'thin', 'open']
        if scifilt1.lower() in skip: scifilt1 = ''
        if scifilt2.lower() in skip: scifilt2 = ''

        filter = ''.join((scifilt1, scifilt2))

        #update keyword
        self.set_keyword('FILTER', filter, 'KOA: set from SCIFILT1 and SCIFILT2')
        return True


    def set_npixsat(self):
        satVal = self.get_keyword('COADDS') * 25000
        return super().set_npixsat(satVal=satVal)


    def set_wavelengths(self):
        '''
        Sets WAVEBLUE, CNTR, RED based on FILTER value
        '''

        log.info('set_wavelengths: setting WAVE keyword values from FILTER')

        filters = {}
        filters['UNKNOWN']   = {'blue':'null', 'cntr':'null', 'red':'null'}
        filters['BLANK']     = {'blue':'null', 'cntr':'null', 'red':'null'}
        filters['NIRSPEC-1'] = {'blue':0.9470, 'cntr':1.0340, 'red':1.1210}
        filters['NIRSPEC-2'] = {'blue':1.0890, 'cntr':1.1910, 'red':1.2930}
        filters['NIRSPEC-3'] = {'blue':1.1430, 'cntr':1.2590, 'red':1.3750}
        filters['NIRSPEC-4'] = {'blue':1.2410, 'cntr':1.4170, 'red':1.5930}
        filters['NIRSPEC-5'] = {'blue':1.4310, 'cntr':1.6195, 'red':1.8080}
        filters['NIRSPEC-6'] = {'blue':1.5580, 'cntr':1.9365, 'red':2.3150}
        filters['NIRSPEC-7'] = {'blue':1.8390, 'cntr':2.2345, 'red':2.6300}
        filters['Br-Gamma']  = {'blue':2.1550, 'cntr':2.1650, 'red':2.1750}
        filters['BR-GAMMA']  = {'blue':2.1550, 'cntr':2.1650, 'red':2.1750}
        filters['CO']        = {'blue':2.2810, 'cntr':2.2930, 'red':2.3050}
        filters['K-PRIME']   = {'blue':1.9500, 'cntr':2.1225, 'red':2.2950}
        filters['KL']        = {'blue':2.1340, 'cntr':3.1810, 'red':4.2280}
        filters['K']         = {'blue':1.9960, 'cntr':2.1890, 'red':2.3820}
        filters['L-PRIME']   = {'blue':3.4200, 'cntr':3.7700, 'red':4.1200}
        filters['M-PRIME']   = {'blue':4.5700, 'cntr':4.6900, 'red':4.8100}
        filters['HEI']       = {'blue':1.0776, 'cntr':1.0830, 'red':1.0884}
        filters['PA-BETA']   = {'blue':1.2757, 'cntr':1.2823, 'red':1.2888}
        filters['FEII']      = {'blue':1.6390, 'cntr':1.6465, 'red':1.6540}
        filters['H2']        = {'blue':2.1100, 'cntr':2.1195, 'red':2.1290}
        filters['M-WIDE']    = {'blue':4.4200, 'cntr':4.9750, 'red':5.5300}

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


    def set_isao(self):
        '''
        Sets the ISAO keyword value: NIRSPEC = no, NIRSPAO = yes 
        '''
        
        log.info('set_isao: setting ISAO keyword values from INSTRUME')

        isao = 'no'
        instrume = self.get_keyword('INSTRUME')
        if instrume == 'NIRSPAO':
            isao = 'yes'

        self.set_keyword('ISAO', isao, 'KOA: Is this NIRSPAO data?')

        return True


    def set_dispers(self):
        '''
        Sets DISPERS, DISPSCAL and SPATSCAL keyword values
        '''

        dispers = 'null'
        dispscal = 'null'
        spatscal = 'null'

        # Set SPATSCAL = PSCALE
        pscale = self.get_keyword('PSCALE')
        if pscale == None: pscale = 'null'
        spatscal = pscale

        if 'NS' in self.get_keyword('KOAID'):
            log.info('set_dispers: setting DISPERS and DISPSCAL keyword values')

            slitname = self.get_keyword('SLITNAME')
            isao = self.get_keyword('ISAO')

            if slitname == None: dispers = 'unknown'
            elif 'x42' in slitname:
                dispers = 'low'
                dispscal = 0.129
                if isao == 'yes': dispscal = 0.012 # 0.129 / 10.6
            else:
                dispers = 'high'
                dispscal = 0.096
                if isao == 'yes': dispscal = 0.009 # 0.096 / 10.6

        #update keywords
        self.set_keyword('DISPERS', dispers, 'KOA: dispersion level')
        self.set_keyword('DISPSCAL', dispscal, 'KOA: pixel scale, dispersion (arcsec/pixel)')
        self.set_keyword('SPATSCAL', spatscal, 'KOA: pixel scale, spatial (arcsec/pixel)')

        return True


    def set_slit_values(self):
        '''
        Sets keyword values defining the slit dimensions
        '''

        slitlen = 'null'
        slitwidt = 'null'
        specres = 'null'

        #low resolution slitwidt:specres
        #y = 4155.1x^2 - 6578.9x + 4400
        lowres = {}
        lowres['0.144'] = 3540
        lowres['0.288'] = 2850
        lowres['0.38'] = 2500
        lowres['0.432'] = 2330
        lowres['0.57'] = 2000
        lowres['0.576'] = 1990
        lowres['0.72'] = 1820
        lowres['0.76'] = 1800
        lowresmap = {}
        lowresmap['0.036'] = 0.38;
        lowresmap['0.054'] = 0.57;
        lowresmap['0.072'] = 0.76;

        #high resolution slitwidtAO:slitwidt
        #y = 10800 / slitwidt
        highresmap = {}
        highresmap['0.0136'] = 0.144
        highresmap['0.0271'] = 0.288
        highresmap['0.0407'] = 0.432
        highresmap['0.0543'] = 0.576
        highresmap['0.0679'] = 0.720
        highresmap['0.0272'] = 0.288
        highresmap['0.0407'] = 0.432
        highresmap['0.0358'] = 0.380
        highresmap['0.0538'] = 0.570
        highresmap['0.0717'] = 0.760
        if self.prefix == 'NS':
            log.info('set_slit_values: setting SLITLEN and SLITWIDT keyword values from SLITNAME')
            slitname = self.get_keyword('SLITNAME')
            if 'x' in slitname:
                #SLITNAME = 42x0.380 (low resolution)
                slitlen, slitwidt = slitname.split('x')

                #SLITNAME = 0.144x12 (high resolution)
                if slitwidt > slitlen:
                    slitlen, slitwidt = slitwidt, slitlen

                dispers = self.get_keyword('DISPERS')
                width = str(slitwidt)
                if dispers == 'low':
                    if self.get_keyword('ISAO') == 'yes':
                        width = lowresmap[width]
                    specres = lowres[str(width.rstrip('0'))]
                elif dispers == 'high':
                    if self.get_keyword('ISAO') == 'yes':
                        width = highresmap[width]
                    specres = int(ceil(10800/float(width)/100.0))*100

                slitlen = float(slitlen)
                slitwidt = float(slitwidt)

        self.set_keyword('SLITLEN', slitlen, 'KOA: Slit length projected on sky (arcsec)')
        self.set_keyword('SLITWIDT', slitwidt, 'KOA: Slit width projected on sky (arcsec)')
        self.set_keyword('SPECRES', specres, 'KOA: Nominal spectral resolution')

        return True


    def set_gain_and_rn(self):
        '''
        Sets the measured values for gain and read noise

        Note from GregD (20190429) - still need to measure RN for SCAM,
        leave null for now
        '''

        gain = 2.85
        readnoise = 'null'

        if self.prefix == 'NS':
            readnoise = 10.8

        self.set_keyword('DETGAIN', gain, 'KOA: Detector gain')
        self.set_keyword('DETRN', readnoise, 'KOA: Detector read noise')

        return True

