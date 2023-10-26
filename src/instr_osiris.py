'''
This is the class to handle all the OSIRIS specific attributes
'''
import instrument
import datetime as dt
from common import *
from math import ceil
import numpy as np
import subprocess

import logging
main_logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class Osiris(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        self.keymap['OFNAME']       = 'DATAFILE'
        self.keymap['FRAMENO']      = 'FRAMENUM'


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',        'crit': True},
            {'name':'set_ut',           'crit': True},
            {'name':'set_elaptime',     'crit': True},
            {'name':'set_filter',       'crit': True},
            {'name':'set_koaimtyp',     'crit': True},
            {'name':'set_frameno',      'crit': True},
            {'name':'set_ofName',       'crit': True},
            {'name':'set_semester',     'crit': True},
            {'name':'set_prog_info',    'crit': True},
            {'name':'set_propint',      'crit': True},
            #{'name':'set_dispers',     'crit': False},
            #{'name':'set_slit_values', 'crit': False},
            {'name':'set_wavelengths',  'crit': False},
            {'name':'set_weather',      'crit': False},
            {'name':'set_wcs_keywords', 'crit': False},
            {'name':'set_image_stats',  'crit': False},
            #{'name':'set_gain_and_rn', 'crit': False},
            {'name':'set_npixsat',      'crit': False},
            {'name':'set_nlinear',      'crit': False},
            {'name':'set_scale',        'crit': False},
            {'name':'check_nonint_vals','crit': False},
            {'name':'set_oa',           'crit': False},
            {'name':'check_ra',         'crit': False},
            {'name':'set_datlevel',     'crit': False,  'args': {'level':0}},
            {'name':'set_dqa_date',     'crit': True},
            {'name':'set_dqa_vers',     'crit': True},
        ]
        return self.run_functions(funcs)


    def get_dir_list(self):
        '''
        Function to generate the paths to all the OSIRIS accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata110'
        for i in range (2):
            seq = (path, str(i))
            path2 = ''.join(seq)
            seq = (path2, '/osiris')
            dirs.append(''.join(seq))
            for j in range(1,21):
                seq = (path2, '/osiris', str(j))
                path3 = ''.join(seq)
                dirs.append(path3)
            seq = (path2, '/osiriseng')
            dirs.append(''.join(seq))
            seq = (path2, '/osrseng')
            dirs.append(''.join(seq))
        return dirs

    def get_prefix(self):
        try:
            instr = self.get_keyword('INSTR', default='')
        except KeyError:
            prefix = ''
        else:
            if 'imag' in instr:
                prefix = 'OI'
            elif 'spec' in instr:
                prefix = 'OS'
            else:
                prefix = ''
        return prefix

    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword
        '''

        #skip it it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        #get necessary keywords
        itime  = self.get_keyword('TRUITIME')
        coadds = self.get_keyword('COADDS')
        #if exposure time or # of exposures doesn't exist, throw error
        if (itime == None or coadds == None):
            self.log_warn("SET_ELAPTIME_ERROR")
            return False

        #update elaptime val (seconds)
        elaptime = round(itime * coadds, 5)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        
        return True
    
    def set_instr(self):
        '''
        Assuming instrument is OSIRIS since INSTRUME not provided in header
        '''
        self.set_keyword('INSTRUME', 'OSIRIS', 'KOA: Instrument')
        return True

    def set_koaimtyp(self):
        '''
        Adds KOAIMTYP keyword
        '''

        koaimtyp = self.get_koaimtyp()

        # warn if undefined
        if koaimtyp == 'undefined':
            main_logger.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")

        # update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')

        return True

    def get_koaimtyp(self):
        '''
        Determines KOAIMTYP keyword value
        '''

        # AO calibrations
        pcuname = self.get_keyword('PCUNAME', default='')
        pcux    = self.get_keyword('PCUX', default=-999)
        pcuy    = self.get_keyword('PCUY', default=-999)
        pcuz    = self.get_keyword('PCUZ', default=-999)
        pcux, pcuy, pcuz = self.check_type_str([pcux, pcuy, pcuz], 0)
        if pcux != 0 and pcuy != 0 and pcuz != 0:
            return 'calib'

        # dark if filter is Drk (set_filter() previously called)
        filter = self.get_keyword('FILTER')
        if 'drk' in filter.lower():
            return 'dark'

        instr    = self.get_keyword('INSTR', default='')
        axestat  = self.get_keyword('AXESTAT', default='')
        domestat = self.get_keyword('DOMESTAT', default='')
        stat = ['tracking', 'slewing']

        # Imager
        if instr.lower() == 'imag':
            flamp1 = self.get_keyword('FLAMP1', default='')
            flamp2 = self.get_keyword('FLAMP2', default='')
            if flamp1.lower() == 'on' or flamp2.lower() == 'on':
                if self.is_at_domeflat():
                    return 'flatlamp'
                else:
                    return 'undefined'
            if domestat in stat and axestat in stat:
                return 'object'
            else:
                if self.is_at_domeflat():
                    return 'flatlampoff' # check for tel position?
            return 'object'

        # Spectrograph
        if instr.lower() == 'spec':
            # recmat files
            datafile = self.get_keyword('DATAFILE')
            if 'c' in datafile:
                return 'calib'
            if domestat in stat and axestat in stat:
                return 'object'

        return 'undefined'

    def set_wcs_keywords(self):
        '''
        Creates WCS keywords
        '''

        crval1 = crval2 = 'null'
        crpix1 = crpix2 = 'null'
        ctype1 = ctype2 = 'null'
        wat0_001 = wat1_001 = wat2_001 = 'null'
        wcsdim = 'null'
        ltm1_1 = ltm2_2 = 'null'
        cdelt1 = cdelt2 = 'null'
        crota2 = 'null'
        radecsys = 'null'

        instr = self.get_keyword('INSTR', default='')
        rotmode = self.get_keyword('ROTMODE')
        poname = self.get_keyword('PONAME')
        rotposn = self.get_keyword('ROTPOSN')
        ra = self.get_keyword('RA')
        dec = self.get_keyword('DEC')

        pi = np.pi

        if instr.lower() == 'imag' and 'position angle' in rotmode:
            main_logger.info('set_wcs_keywords: setting WCS keyword values')
            ctype1 = 'RA---TAN'
            ctype2 = 'DEC--TAN'
            wat0_001 = 'system=image'
            wat1_001 = 'wtype=tan axtype=ra'
            wat2_001 = 'wtype=tan axtype=dec'
            wcsdim = 2
            radecsys = 'FK5'
            ltm1_1 = 1.000
            ltm2_2 = 1.000
            if 'ospec' in poname.lower():
                offset = 48
                theta = (offset - (rotposn+90)) * pi / 180.0
                deltaRA = (15.42 * np.cos(theta) + 14.12 * np.sin(theta)) / (np.cos(dec*pi/180.0)*3600.0)
                deltaDEC = (15.42 * np.sin(theta) - 14.12 * np.cos(theta)) / 3600.0
                crval1 = round(ra + deltaRA, 5)
                crval2 = round(dec + deltaDEC, 5)
            elif 'osimg' in poname.lower():
                deltaRA = 0.0
                deltaDEC = 0.0
                crval1 = round(ra, 5)
                crval2 = round(dec, 5)

            if crval1 != 'null':
                crota2 = (rotposn+90)
                while crota2 < 0: crota2 += 360.0
                cdelt1 = -0.000002777778 # 10 miliarcseconds/pixel
                cdelt2 = 0.0000027777778 # 10 miliarcseconds/pixel
                crpix1 = 512.5
                crpix2 = 512.5

        self.set_keyword('CRVAL1', crval1, 'KOA: WCS value at the reference pixel')
        self.set_keyword('CRVAL2', crval2, 'KOA: WCS value at the reference pixel')
        self.set_keyword('CRPIX1', crpix1, 'KOA: Reference pixel on the horizontal axis')
        self.set_keyword('CRPIX2', crpix2, 'KOA: Reference pixel on the vertical axis')
        self.set_keyword('CTYPE1', ctype1, 'KOA: WCS Type of the horizontal coordinate')
        self.set_keyword('CTYPE2', ctype2, 'KOA: WCS Type of the vertical coordinate')
        self.set_keyword('WAT0_001', wat0_001, 'KOA: coordinate system')
        self.set_keyword('WAT1_001', wat1_001, 'KOA: coordinate system')
        self.set_keyword('WAT2_001', wat2_001, 'KOA: coordinate system')
        self.set_keyword('WCSDIM', wcsdim, 'KOA: number of WCS dimensions')
        self.set_keyword('LTM1_1', ltm1_1, 'KOA: ccd to image transformation')
        self.set_keyword('LTM2_2', ltm2_2, 'KOA: ccd to image transformation')
        self.set_keyword('CDELT1', cdelt1, '')
        self.set_keyword('CDELT2', cdelt2, '')
        self.set_keyword('CROTA2', crota2, '')
        self.set_keyword('RADECSYS', radecsys, 'KOA: the system of the coordinates')

        return True


    def set_filter(self):
        '''
        Populates filter from ifilter or sfilter
        '''
        instr = self.get_keyword('INSTR', default='')
        ifilter = self.get_keyword('IFILTER', default='')
        sfilter = self.get_keyword('SFILTER', default='')

        filter = ''
        if instr.lower() == 'imag':
            filter = ifilter
        elif instr.lower() == 'spec':
            filter = sfilter

        self.set_keyword('FILTER', filter, 'KOA: Copy of IFILTER/SFILTER')

        return True


    def set_npixsat(self):
        '''Determines number of saturated pixels and adds NPIXSAT to header'''
        coadds = self.get_keyword('COADDS')
        saturate = self.get_keyword('SATURATE')
        if coadds is None or saturate is None:
            self.log_warn("SET_NPIXSAT_ERROR", f'{coadds}, {saturate}')
            return False

        satval = coadds * saturate
        return super().set_npixsat(satVal=satval, ext=0)


    def set_wavelengths(self):
        '''
        Set wavelength values based off filters used
        '''
        waveblue = wavecntr = wavered = 'null'

        wave = {}
        wave['zbb']     = {'waveblue': 999, 'wavered':1176}
        wave['jbb']     = {'waveblue':1180, 'wavered':1440}
        wave['hbb']     = {'waveblue':1473, 'wavered':1803}
        wave['kbb']     = {'waveblue':1965, 'wavered':2381}
        wave['kcb']     = {'waveblue':1965, 'wavered':2381}
        wave['zn4']     = {'waveblue':1103, 'wavered':1158}
        wave['jn1']     = {'waveblue':1174, 'wavered':1232}
        wave['jn2']     = {'waveblue':1228, 'wavered':1289}
        wave['jn3']     = {'waveblue':1275, 'wavered':1339}
        wave['jn4']     = {'waveblue':1323, 'wavered':1389}
        wave['hn1']     = {'waveblue':1466, 'wavered':1541}
        wave['hn2']     = {'waveblue':1532, 'wavered':1610}
        wave['hn3']     = {'waveblue':1594, 'wavered':1676}
        wave['hn4']     = {'waveblue':1652, 'wavered':1737}
        wave['hn5']     = {'waveblue':1721, 'wavered':1808}
        wave['kn1']     = {'waveblue':1955, 'wavered':2055}
        wave['kn2']     = {'waveblue':2036, 'wavered':2141}
        wave['kn3']     = {'waveblue':2121, 'wavered':2229}
        wave['kc3']     = {'waveblue':2121, 'wavered':2229}
        wave['kn4']     = {'waveblue':2208, 'wavered':2320}
        wave['kc4']     = {'waveblue':2208, 'wavered':2320}
        wave['kn5']     = {'waveblue':2292, 'wavered':2408}
        wave['kc5']     = {'waveblue':2292, 'wavered':2408}
        wave['pagamma'] = {'waveblue':1087, 'wavered':1105}
        wave['feii']    = {'waveblue':1634, 'wavered':1661}
        wave['hcont']   = {'waveblue':1571, 'wavered':1596}
        wave['zn3']     = {'waveblue':1061, 'wavered':1113}
        wave['y']       = {'waveblue': 977, 'wavered':1073}
        wave['j']       = {'waveblue':1168, 'wavered':1318}
        wave['kp']      = {'waveblue':1961, 'wavered':2268}
        wave['brgamma'] = {'waveblue':2155, 'wavered':2184}
        wave['kcont']   = {'waveblue':2259, 'wavered':2281}
        wave['hei_b']   = {'waveblue':2046, 'wavered':2075}

        filter = self.get_keyword('FILTER', default='')
        filter = filter.lower()

        if filter in wave.keys():
            waveblue = wave[filter]['waveblue']
            wavered = wave[filter]['wavered']
            wavecntr = int((wavered + waveblue) / 2.0)

        self.set_keyword('WAVEBLUE', waveblue, 'KOA: Approximate blue end wavelength (nm)')
        self.set_keyword('WAVECNTR', wavecntr, 'KOA: Approximate central wavelength (nm)')
        self.set_keyword('WAVERED', wavered, 'KOA: Approximate red end wavelength (nm)')

        return True

    def set_nlinear(self, satVal=None):
        '''
        Determines number of saturated pixels above linearity, adds NLINEAR to header
        '''
        if satVal == None:
            satVal = self.get_keyword('SATURATE')
        if satVal == None:
            self.log_warn("SET_NLINEAR_ERROR")
            return False

        satVal = 0.8 * satVal * self.get_keyword('COADDS')
        image = self.fits_hdu[0].data     
        linSat = image[np.where(image >= satVal)]
        nlinSat = len(image[np.where(image >= satVal)])
        self.set_keyword('NLINEAR', nlinSat, 'KOA: Number of pixels above linearity')
        self.set_keyword('NONLIN', int(satVal), 'KOA: 3% nonlinearity level (80% full well)')
        return True

    def set_scale(self):
        '''
        Sets scale
        '''       
        sscale = self.get_keyword('SSCALE')
        instr = self.get_keyword('INSTR')
        if "imag" in instr:
            scale = 0.02
        else:
            scale = sscale
        self.set_keyword('SCALE', scale, 'KOA: Scale')
        return True

    def check_nonint_vals(self):
        '''
        This checks certain keywords for decimal values less than one and converts them to zero.
        NOTE: This is a direct port from old IDL code.  Not sure what it is for.
        '''
        kws = ['SHTRANG', 'SHTRACT', 'IHTRACT']
        for kw in kws:
            val = self.get_keyword(kw)
            if not val: continue
            val = float(val)
            if val < 1: 
                val = 0
                self.set_keyword(kw, val)
        return True


    def check_ra(self):
        '''
        If KOAIMTYP=calib and (RA<-720 or RA>720), then RA=null
        '''

        koaimtyp = self.get_keyword('KOAIMTYP')
        ra = self.get_keyword('RA')

        if ra == None:
            return True

        if koaimtyp == 'calib' and (float(ra) < -720 or float(ra) > 720):
            main_logger.info('check_ra: changing RA to null')
            self.set_keyword('RA', None)

        return True


    def run_drp(self):
        '''
        Run the OSIRIS DRP on vm-koaserver2
        '''

        drp = self.config.get(self.instr, {}).get('DRP')
        if not drp:
            main_logger.info("No DRP defined.")
            return True

        cmd = []
        for word in self.config[self.instr]['DRP'].split(' '):
            cmd.append(word)
        cmd.append(self.utdate)

        main_logger.info(f'run_drp: Running DRP command: {" ".join(cmd)}')
        p = subprocess.Popen(cmd)
        p.wait()
        main_logger.info('run_drp: DRP finished')

        return True


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP sepecific to OSIRIS.
 
        QL ingest (level 1): KOAID*

        Science (level 2): ?
        '''

        files = []

        if level == 1:
            searchfiles = [
                f'{datadir}/{koaid}.lev1.fits',
                f'{datadir}/{koaid}.lev1.log'
            ]
            for f in searchfiles:
                if os.path.isfile(f): files.append(f)

        if len(files) == 0:
            return False

        return files


    def get_drp_destfiles(self, koaid, srcfile):
        '''
        Returns the destination of the DRP file for RTI.
        For OSIRIS, destination = source file.
        '''

        return True, srcfile


    def has_target_info(self):
        '''
        Does this fits have sensitive target info?
        '''
        return False


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP specific to OSIRIS.

        QL ingest (KOA level 1)
            IMAGE: KOAID_drp.[fits.gz,jpg]
            SPEC:  KOAID*.lev1.[fits.gz,jpg]

        Science ingest (KOA level 2)
            SPEC:  KOAID.lev2[.fits.gz,_median.jpg,_sum_positive_slices.jpg]
        '''
        files = []

        searchfiles = [
            f"{datadir}/{koaid}_drp.fits.gz",
            f"{datadir}/{koaid}_drp.jpg",
            f"{datadir}/{koaid}.lev1.fits.gz",
            f"{datadir}/{koaid}.lev1.jpg",
            f"{datadir}/{koaid}.lev2.fits.gz",
            f"{datadir}/{koaid}.lev2_median.jpg",
            f"{datadir}/{koaid}.lev2_sum_positive_slices.jpg"
        ]
        for f in searchfiles:
            print(f)
            if os.path.isfile(f): files.append(f)

        if len(files) == 0:
            return False

        return files


    def get_drp_destfile(self, koaid, srcfile):
        '''
        Returns the destination of the DRP file for RTI.
        For OSIRIS, destination = source file.
        '''

        return True, srcfile

