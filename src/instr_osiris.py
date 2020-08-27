'''
This is the class to handle all the OSIRIS specific attributes
OSIRIS specific DR techniques can be added to it in the future

12/14/2017 M. Brown - Created initial file
'''
import instrument
import datetime as dt
from common import *
from math import ceil
import numpy as np

class Osiris(instrument.Instrument):

    def __init__(self, instr, utDate, rootdir, log=None):

        # Call the parent init to get all the shared variables
        super().__init__(instr, utDate, rootdir, log)

        # Set any unique keyword index values here
        self.keymap['OFNAME']       = 'DATAFILE'
        self.keymap['FRAMENO']      = 'FRAMENUM'

        #other vars that subclass can overwrite
        self.endTime = '19:00:00'   # 24 hour period start/end time (UT)

        # Generate the paths to the OSIRIS datadisk accounts
        self.paths = self.get_dir_list()


    def run_dqa(self, progData):
        '''
        Run all DQA checks unique to this instrument
        '''
        ok = True
        if ok: ok = self.set_dqa_date()
        if ok: ok = self.set_dqa_vers()
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.set_ut()
        if ok: ok = self.set_elaptime()
        if ok: ok = self.set_koaimtyp()
        if ok: ok = self.set_frameno()
        if ok: ok = self.set_filter()
        if ok: ok = self.set_ofName()
        if ok: ok = self.set_semester()
#        if ok: ok = self.set_dispers()
#        if ok: ok = self.set_slit_values()
        if ok: ok = self.set_wavelengths()
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_wcs_keywords()
        if ok: ok = self.set_image_stats_keywords()
#        if ok: ok = self.set_gain_and_readnoise()
        if ok: ok = self.set_npixsat(self.get_keyword('COADDS')*self.get_keyword('SATURATE'))
        if ok: ok = self.set_nlinear()
        if ok: ok = self.set_scale()
        if ok: ok = self.check_noninteger_values()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_prog_info(progData)
        if ok: ok = self.set_propint(progData)
        if ok: ok = self.check_propint()
        if ok: ok = self.check_ra()

        return ok


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
            instr = self.get_keyword('INSTR')
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
        log.info('set_elaptime: determining ELAPTIME from TRUITIME')

        #skip it it exists
        if self.get_keyword('ELAPTIME', False) != None: return True

        #get necessary keywords
        itime  = self.get_keyword('TRUITIME')
        coadds = self.get_keyword('COADDS')
        #if exposure time or # of exposures doesn't exist, throw error
        if (itime == None or coadds == None):
            log.error('set_elaptime: TRUITIME and COADDS values needed to set ELAPTIME')
            return False

        #update elaptime val (seconds)
        elaptime = round(itime * coadds, 5)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        
        return True
    
    def set_instr(self):
        '''
        Assuming instrument is OSIRIS since INSTRUME not provided in header
        '''

        log.info('set_instr: setting INSTRUME to OSIRIS')
        #update instrument
        self.set_keyword('INSTRUME', 'OSIRIS', 'KOA: Instrument')
        
        return True
        

    def set_koaimtyp(self):
        '''
        Adds KOAIMTYP keyword
        '''
        log.info('set_koaimtyp: setting KOAIMTYP keyword from algorithm')

        koaimtyp = 'undefined'
        ifilter = self.get_keyword('IFILTER', default='')
        sfilter = self.get_keyword('SFILTER', default='')
        axestat = self.get_keyword('AXESTAT', default='')
        domeposn = self.get_keyword('DOMEPOSN', default=0)
        az = self.get_keyword('AZ', default=0)
        el = self.get_keyword('EL', default=0)
        obsfname = self.get_keyword('OBSFNAME', default='')
        obsfx = self.get_keyword('OBSFX')
        obsfy = self.get_keyword('OBSFY')
        obsfz = self.get_keyword('OBSFZ')
        instr = self.get_keyword('INSTR')
        datafile = self.get_keyword('DATAFILE')
        coadds = self.get_keyword('COADDS')

        #TODO: NOTE: Various values of above keywords can be of mixed type, 
        #such as "###ERROR###" for 'el' instead of a float.  Just wrapping in try
        #and logging as error for now until we can handle this more cleanly.
        try: 
            # telescope at flat position
            flatpos = 0
            if (el < 45.11 and el > 44.89) and (domeposn - az > 80.0 and domeposn - az < 100.0):
                flatpos = 1

            if 'telescope' in obsfname.lower():
                koaimtyp = 'object'

            # recmat files
            if 'c' in datafile:
                koaimtyp = 'calib'

            # dark if ifilter/sfilter is dark
            if 'drk' in ifilter.lower() and instr.lower() == 'imag':
                koaimtyp = 'dark'
            elif 'drk' in sfilter.lower() and instr.lower() == 'spec':
                koaimtyp = 'dark'

            # uses dome lamps for instr=imag
            elif instr.lower() == 'imag':
                if 'telescope' in obsfname and 'not controlling' in axestat and flatpos:
                    # divide image by coadds
                    img = self.fits_hdu[0].data

                    # median
                    imgmed = np.median(img)

                    if imgmed > 30.0:
                        koaimtyp = 'flatlamp'
                    else:
                        koaimtyp = 'flatlampoff'

            if instr.lower == 'spec':
                if 'telsim' in obsfname or (obsfx > 30 and obsfy < 0.1 and obsfz < 0.1):
                    koaimtyp = 'undefined'
                elif obsfz > 10:
                    koaimtyp = 'undefined'
            elif 'c' in datafile:
                koaimtyp = 'calib'
        except Exception as e:
            log.error('set_koaimtype: ' + str(e))

        #warn if undefined
        if (koaimtyp == 'undefined'):
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')

        #update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        return True


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

        instr = self.get_keyword('INSTR')
        rotmode = self.get_keyword('ROTMODE')
        poname = self.get_keyword('PONAME')
        rotposn = self.get_keyword('ROTPOSN')
        ra = self.get_keyword('RA')
        dec = self.get_keyword('DEC')

        pi = np.pi

        if instr.lower() == 'imag' and 'position angle' in rotmode:
            log.info('set_wcs_keywords: setting WCS keyword values')
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
                offset = 47.5
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
                crota2 = -(rotposn+90)
                while crota2 < 0: crota2 += 360.0
                cdelt1 = -0.0000055555556
                cdelt2 = 0.0000055555556
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

        log.info('set_wavelengths: setting FILTER keyword value')

        instr = self.get_keyword('INSTR')
        ifilter = self.get_keyword('IFILTER', default='')
        sfilter = self.get_keyword('SFILTER', default='')

        filter = ''
        if instr.lower() == 'imag':
            filter = ifilter
        elif instr.lower() == 'spec':
            filter = sfilter

        self.set_keyword('FILTER', filter, 'KOA: Copy of IFILTER/SFILTER')

        return True


    def set_wavelengths(self):
        '''
        Set wavelength values based off filters used
        '''

        log.info('set_wavelengths: setting WAVE keyword values from FILTER')

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

        instr = self.get_keyword('INSTR')
        filter = self.get_keyword('FILTER')
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

        log.info('set_nlinear: setting number of pixels above linearity keyword value')

        if satVal == None:
            satVal = self.get_keyword('SATURATE')
            
        if satVal == None:
            log.warning("set_nlinear: Could not find SATURATE keyword")
        else:
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

        log.info('set_scale: setting SCALE from SSCALE')

        sscale = self.get_keyword('SSCALE')
        instr = self.get_keyword('INSTR')
        
        if "imag" in instr:
            scale = 0.02
        else:
            scale = sscale
        
        self.set_keyword('SCALE', scale, 'KOA: Scale')
        
        return True

    def check_noninteger_values(self):
        '''
        This checks certain keywords for decimal values less than one and converts them to zero.
        NOTE: This is a direct port from old IDL code.  Not sure what it is for.
        '''

        log.info('check_noninteger_values: checking SHTRANG, SHTRACT and IHTRACT')

        kws = ['SHTRANG', 'SHTRACT', 'IHTRACT']
        for kw in kws:
            val = self.get_keyword(kw)
            if not val: continue
            val = float(val)
            if val < 1: 
                val = 0
                self.set_keyword(kw, val)
        return True


    def check_propint(self):
        '''
        Change propint to 0 for PROGID=ENG and KOAIMTYP=calib
        '''

        koaimtyp = self.get_keyword('KOAIMTYP')
        progid = self.get_keyword('PROGID')

        if progid == 'ENG' and koaimtyp == 'calib':
            pp = self.extra_meta['PROPINT']
            log = 'check_propint: Changing PROPINT from ' + str(pp) + ' to 0'
            log.info(log)
            self.extra_meta['PROPINT'] = 0

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
            log.info('check_ra: changing RA to null')
            self.set_keyword('RA', None)

        return True


    def run_drp(self):
        '''
        Run the OSIRIS DRP on vm-koaserver2
        '''

        import subprocess

        cmd = []
        for word in self.config[self.instr]['DRP'].split(' '):
            cmd.append(word)
        cmd.append(self.utdate)

        log.info(f'run_drp: Running DRP command: {" ".join(cmd)}')
        p = subprocess.Popen(cmd)
        p.wait()
        log.info('run_drp: DRP finished')

        return True

