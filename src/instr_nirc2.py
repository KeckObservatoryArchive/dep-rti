'''
This is the class to handle all the NIRC2 specific attributes
'''

import instrument
import datetime as dt
import numpy as np
import scipy.stats
import os
import subprocess
from socket import gethostname

import logging
log = logging.getLogger('koadep')


class Nirc2(instrument.Instrument):

    def __init__(self, instr, filepath, config, db, reprocess, tpx):

        super().__init__(instr, filepath, config, db, reprocess, tpx)

        # Set any unique keyword index values here
        # NIRC2 uses ROOTNAME instead of OUTDIR
        self.ofName = 'FILENAME'


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        ok=True
        if ok: ok = super().run_dqa()
        if ok: ok = self.set_dqa_date()
        if ok: ok = self.set_dqa_vers()
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.set_ut() # may need to delete duplicate UTC?
        if ok: ok = self.set_koaimtyp() # imagetyp
        if ok: ok = self.set_semester()
        if ok: ok = self.set_wavelengths()
        if ok: ok = self.set_detdisp()
        if ok: ok = self.set_wcs()
        if ok: ok = self.set_elaptime()
        if ok: ok = self.set_ofname()
        if ok: ok = self.set_instrument_status() # inststat
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_image_stats_keywords() # IM* and PST*, imagestat
        if ok: ok = self.set_npixsat(satVal = self.get_keyword('COADDS')*18000.0) # npixsat
        if ok: ok = self.set_nlinear(satVal = self.get_keyword('COADDS')*5000.0)
        if ok: ok = self.set_sig2nois()
        if ok: ok = self.set_isao()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_prog_info()
        if ok: ok = self.set_propint()
        return ok


    def get_dir_list(self):
        '''
        Function to generate the paths to all the NIRC2 accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata90'
        for i in range(5):
            joinSeq = (path, str(i), '/nirc')
            path2 = ''.join(joinSeq)
            for j in range(1,11):
                joinSeq = (path2, str(j))
                path3 = ''.join(joinSeq)
                dirs.append(path3)
            joinSeq = (path2, '2eng')
            path3 = ''.join(joinSeq)
            dirs.append(path3)
        return dirs

    def get_prefix(self):
        if self.get_keyword('INSTRUME') == self.instr:
            prefix = 'N2'
        else:
            prefix = ''
        return prefix

    def set_instr(self):
        '''
        Check OUTDIR to verify NIRC2 and add INSTRUME
        '''

        log.info('set_instr: setting INSTRUME to NIRC2')
        if 'nirc' in self.get_keyword('OUTDIR'):
            #update instrument
            self.set_keyword('INSTRUME', 'NIRC2', 'KOA: Instrument')

        return True

    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm
        Calls get_koaimtyp for algorithm
        '''

        koaimtyp = self.get_koaimtyp()
        if koaimtyp in ['flatTBD','specTBD','telTBD']:
            koaimtyp = self.set_caltype(koaimtyp)
        #warn if undefined
        if (koaimtyp == 'undefined'):
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')

        #update keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        
        return True

        
    def get_koaimtyp(self):
        '''
        Sets koaimtyp based on keyword values
        '''
        #define python replica of IDL strtrim
        #strtrim = lambda x: x.replace(' ','')

        grsname = self.get_keyword('GRSNAME')
        shrname = self.get_keyword('SHRNAME')
        obsfname = self.get_keyword('OBSFNAME')
        domestat = self.get_keyword('DOMESTAT')
        axestat = self.get_keyword('AXESTAT')
        imagetyp = 'undefined'
        #shutter open
        if shrname == 'open':
            if obsfname == 'telescope':
                imagetyp = 'object'
            if (obsfname == 'telescope') and (domestat != 'tracking') and (axestat != 'tracking'):
                if grsname != 'clear':
                    imagetyp = 'telTBD'
                    return imagetyp
                #if domelamps keyword exists
                if self.get_keyword('FLIMAGIN'):
                    flspectr = self.get_keyword('FLSPECTR')
                    flimagin = self.get_keyword('FLIMAGIN')
                    if flimagin == 'on' or flspectr == 'on':
                        imagetyp = 'flatlamp'
                    else:
                        imagetyp = 'flatlampoff'
                #else check EL, AXESTAT, and DOMESTAT instead
                else:
                    el = float(self.get_keyword('EL'))
                    if (el > 44.99 and el < 45.01) and (domestat != 'tracking' and axestat != 'tracking'):
                        imagetyp = 'flatTBD'

            #arclamp
            elif obsfname == 'telsim':
                if self.get_keyword('ARGONPWR'):
                    #get element power boolean
                    argonpwr = self.get_keyword('ARGONPWR')
                    xenonpwr = self.get_keyword('XENONPWR')
                    kryptpwr = self.get_keyword('KRYPTPWR')
                    neonpwr = self.get_keyword('NEONPWR')
                    lamppwr = self.get_keyword('LAMPPWR')
                    #compare dates for special logic after 2011-10-10
                    dateobs = self.get_keyword('DATE-OBS')
                    date = dateobs.split('-')
                    dateval = dt.date(int(date[0]),int(date[1]),int(date[2]))
                    dlmpvalid = dt.date(2011,10,10)
                    if dateval > dlmpvalid:
                        if lamppwr == 1:
                            print("lamppower")
                            imagetyp = 'flatlamp'
                        elif 1 in [argonpwr,xenonpwr,kryptpwr,neonpwr]:
                            imagetyp = 'arclamp'
                        else:
                            imagetyp = 'specTBD'
                        print('Image Type: ',imagetyp)
                        print('Lamp Power: ',lamppwr)
                        print('Ne: ',neonpwr)
                        print('Ar: ',argonpwr)
                        print('Kr: ',kryptpwr)
                        print('Xe: ',xenonpwr)
            #use grsname keyword instead
            else:
                if grsname in ['lowres','medres','GRS1','GRS2']:
                    imagetyp = 'specTBD'
        #dark or bias
        elif shrname == 'closed':
            itime = self.get_keyword('ITIME')
            if itime == 0.0:
                imagetyp = 'bias'
            else:
                imagetyp = 'dark'
            print('Image Type: ',imagetyp)

#        log.info('get_koaimtyp: getting KOAIMTYP')
        return imagetyp

    def set_wavelengths(self):
        '''
        Sets WAVERED, WAVEBLUE, and WAVECEN
        '''
        #get current wave values from header
        maxwave = float(self.get_keyword('MAXWAVE'))
        minwave = float(self.get_keyword('MINWAVE'))
        cenwave = float(self.get_keyword('CENWAVE'))
        #if minwave=0.0 and maxwave=5.0, set minwave=0.9
        if minwave == 0.0:
            if maxwave == 5.0:
                minwave = 0.9
            #if min/max/center=0, set to null
            elif maxwave == 0 and cenwave == 0:
                maxwave='null'
                minwave='null'
                cenwave='null'
            else:
                minwave='null'

        self.set_keyword('WAVERED',maxwave,'KOA: Maximum Wavelength')
        self.set_keyword('WAVEBLUE',minwave,'KOA: Minimum Wavelength')
        self.set_keyword('WAVECNTR',cenwave,'KOA: Center Wavelength')
        log.info('set_wavelengths: setting WAVERED, WAVEBLUE, WAVECNTR')
        return True

    def set_detdisp(self):
        '''
        Sets detector mode, gain, read noise and dispersion
        '''
        detmode = 'null'
        disp = 'unknown'
        #get grsname
        if self.get_keyword('GRSNAME'):
            #logic for detmode
            grsname = (self.get_keyword('GRSNAME')).replace(' ','')
            if grsname in ['lowres','medres','GR1','GR2']:
                detmode = 'spec'
            else:
                detmode = 'image'
            #logic for disp
            if grsname == 'lowres':
                disp = 'low'
            elif grsname == 'medres':
                disp = 'medium'

        self.set_keyword('DETMODE',detmode,'KOA: Detector Mode')
        self.set_keyword('DISPERS',disp,'KOA: Dispersion')
        self.set_keyword('DETGAIN',4,'KOA: Detector Gain')
        self.set_keyword('DETRN',39,'KOA: Detector Read Noise')
        log.info('set_detdisp: setting DETMODE, DETGAIN, DETRN, DISPERS')
        return True

    def set_wcs(self):
        '''
        Set the WCS keywords for NIRC2 images
        '''

        log.info('set_wcs: Setting WCS keywords')

        pixscale = radecsys = wcsdim = 'null'
        crval1 = crval2 = crpix1 = crpix2 = 'null'
        cd1_1 = cd1_2 = cd2_1 = cd2_2 = 'null'
        ltm1_1 = ltm2_2 = 'null'
        ctype1 = ctype2 = 'null'
        wat0_001 = wat1_001 = wat2_001 = 'null'

        # Get header keywords

        rakey    = self.get_keyword('RA')
        deckey   = self.get_keyword('DEC')
        equinox  = self.get_keyword('EQUINOX')
        camname  = self.get_keyword('CAMNAME', default='')
        naxis1   = self.get_keyword('NAXIS1')
        naxis2   = self.get_keyword('NAXIS2')
        pa       = self.get_keyword('ROTPOSN')
        rotmode  = self.get_keyword('ROTMODE')
        parantel = self.get_keyword('PARANTEL', default='')
        parang   = self.get_keyword('PARANG')
        el       = self.get_keyword('EL')

        if isinstance(parantel, str) and (parantel == '' or 'error' in parantel.lower()): parantel = parang 
        mode =  rotmode[0:4]
       
        # Logic added 4/16/2012 
        # special PA calculation determine by rotmode below  
        # instead of one formula   pa = double ( rotpposn + parantel - el )

        paCalc = lambda x,y,z: float(x) + float(y) - float(z)
        if mode in ['posi', 'vert', 'stat'] and camname in ['narrow', 'medium', 'wide']:
            if mode == 'posi':   
                pa1 = paCalc(pa, 0, 0)
            elif mode == 'vert': 
                pa1 = paCalc(pa, parantel, 0)
            elif mode == 'stat': 
                pa1 = paCalc(pa, parantel, el)

            raindeg = 1

            # Pixel scale and PA offset by camera name

            pixscale = {'narrow':0.009952, 'medium':0.019829, 'wide':0.039686}
            pazero = {'narrow':0.448, 'medium':0.7, 'wide':0.7} # narrow = 0.7-0.252, correction from Yelda etal 2010

            crval1 = rakey
            crval2 = deckey


            sign = 1
            pa = pa1 - pazero[camname]
            pa *= np.pi / 180.0
            pa *= -1.0

            cd1_1 = -sign * pixscale[camname] * np.cos(pa) / 3600.0
            cd2_2 =  sign * pixscale[camname] * np.cos(pa) / 3600.0
            cd1_2 = -sign * pixscale[camname] * np.sin(pa) / 3600.0
            cd2_1 = -sign * pixscale[camname] * np.sin(pa) / 3600.0

            pixscale = '%f' % round(pixscale[camname], 6)

            cd1_1 = '%0.12lf' % round(cd1_1, 12)
            cd1_2 = '%0.12lf' % round(cd1_2, 12)
            cd2_1 = '%0.12lf' % round(cd2_1, 12)
            cd2_2 = '%0.12lf' % round(cd2_2, 12)

            crpix1 = round(float((naxis1 + 1) / 2.0), 2)
            crpix2 = round(float((naxis2 + 1) / 2.0), 2)
     
            # check the equinox
            # fk4 = 1950
            # fk5 = 2000
            if equinox == 2000: 
                radecsys = 'FK5'
            else:               
                radecsys = 'FK4'
            # Fixed values
            wcsdim = 2
            ltm1_1 = ltm2_2 = 1.0
            ctype1 = 'RA---TAN'
            ctype2 = 'DEC--TAN'
            wat0_001 = 'system=image'
            wat1_001 = 'wtype=tan axtype=ra'
            wat2_001 = 'wtype=tan axtype=dec'

        # Add header keywords

        self.set_keyword('PIXSCALE', pixscale, 'KOA: Pixel scale')
        self.set_keyword('PIXSCAL1', pixscale, 'KOA: Pixel scale, horizontal axis')
        self.set_keyword('PIXSCAL2', pixscale, 'KOA: Pixel scale, vertical axis')
        self.set_keyword('CRPIX1', crpix1, 'KOA: Reference pixel, horizontal axis')
        self.set_keyword('CRPIX2', crpix2, 'KOA: Reference pixel, vertical axis')
        self.set_keyword('CRVAL1', crval1, 'KOA: WCS value at the reference pixel, horizontal axis')
        self.set_keyword('CRVAL2', crval2, 'KOA: WCS value at the reference pixel, vertical axis')
        self.set_keyword('CTYPE1', ctype1, 'KOA: WCS Type, horizontal coordinate')
        self.set_keyword('CTYPE2', ctype2, 'KOA: WCS Type, vertical coordinate')
        self.set_keyword('WAT0_001', wat0_001, 'KOA: coordinate system')
        self.set_keyword('WAT1_001', wat1_001, 'KOA: coordinate system')
        self.set_keyword('WAT2_001', wat2_001, 'KOA: coordinate system')
        self.set_keyword('WCSDIM', wcsdim, 'KOA: Number of WCS dimensions')
        self.set_keyword('LTM1_1', ltm1_1, 'KOA: CCD to image transformation')
        self.set_keyword('LTM2_2', ltm2_2, 'KOA: CCD to image transformation')
        self.set_keyword('CD1_1', cd1_1, 'KOA: Coordinate transformation matrix')
        self.set_keyword('CD1_2', cd1_2, 'KOA: Coordinate transformation matrix') 
        self.set_keyword('CD2_1', cd2_1, 'KOA: Coordinate transformation matrix') 
        self.set_keyword('CD2_2', cd2_2, 'KOA: Coordinate transformation matrix') 
        self.set_keyword('RADECSYS', radecsys, 'KOA: The system of the coordinates')
        return True

    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword
        '''
        log.info('set_elaptime: determining ELAPTIME from ITIME')

        #skip it it exists
        if self.get_keyword('ELAPTIME', False) != None: 
            return True

        #get necessary keywords
        itime  = self.get_keyword('ITIME')
        coadds = self.get_keyword('COADDS')
        #if exposure time or # of exposures doesn't exist, throw error
        if (itime == None or coadds == None):
            log.error('set_elaptime: ITIME and COADDS values needed to set ELAPTIME')
            return False

        #update elaptime val (seconds)
        elaptime = round(itime * coadds, 5)
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        
        return True

    def set_ofname(self):
        '''
        Sets OFNAME to value of FILENAME
        '''
        log.info('set_ofname: setting OFNAME = FILENAME')
        self.set_keyword('OFNAME',self.get_keyword('FILENAME'),'KOA: Original Filename')

        return True

    def set_instrument_status(self):
        ''' 
        Sets instrument status
        '''
        inststat = 0
        #check PSINAME and PSONAME
        if self.get_keyword('PSINAME') and self.get_keyword('PSONAME'):
            psiname = (self.get_keyword('PSINAME')).replace(' ','')
            psoname = (self.get_keyword('PSONAME')).replace(' ','')
            if psiname == 'blank_center' or psoname == 'blank_center':
                inststat = 0
            else:
                inststat = 1
        else:
            inststat = -1

        self.set_keyword('INSTSTAT',inststat,'KOA: Instrument Status')

        return True
        

    def set_isao(self):
        '''
        Sets AO status
        '''
        self.set_keyword('ISAO','yes','KOA: AO status')
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
            image = self.fits_hdu[0].data     
            linSat = image[np.where(image >= satVal)]
            nlinSat = len(image[np.where(image >= satVal)])
            self.set_keyword('NLINEAR', nlinSat, 'KOA: Number of pixels above linearity')
            self.set_keyword('NONLIN', int(satVal), 'KOA: 3% nonlinearity level (80% full well)')

        return True

    def set_caltype(self,imagetyp):
        image = self.fits_hdu[0].data
        imgmean = np.mean(image)
        imgstdv = np.std(image)
        krtosis = scipy.stats.kurtosis(image, axis=None)
        print(imgmean,imgstdv,krtosis)
        #determine lamp when 'flatTBD'
        if imagetyp == "flatTBD":
            if imgmean > 500:
                imagetyp = 'flatlamp'
            else:
                imagetyp = 'flatlampoff'
            print('flatTBD => '+imagetyp)
        #determine lamp for spectra when 'specTBD'
        elif imagetyp == "specTBD":
            imagetyp = 'undefined'
            if imgmean < 1000:
                if imgstdv > 1000 and krtosis < 300:
                    imagetyp = 'arclamp'
                elif imgstdv < 300 and krtosis > 300:
                    imagetyp = 'flatlampoff'
            elif imgmean < 10000:
                imagetyp = 'flatlamp'
            print('specTBD => '+imagetyp)
        #determine lamp for spectra when 'telTBD'
        elif imagetyp == "telTBD":
            imagetyp = 'undefined'
            if imgmean < 1000:
                if imgstdv < 300 and krtosis > 300:
                    imagetyp = 'flatlampoff'
            elif imgmean < 10000:
                imagetyp = 'flatlamp'
            print('telTBD => '+imagetyp)

        return imagetyp


    def set_sig2nois(self):
        '''
        Calculates S/N for CCD image
        '''

        log.info('set_sig2nois: Adding SIG2NOIS')

        image = self.fits_hdu[0].data

        naxis1 = self.get_keyword('NAXIS1')
        naxis2 = self.get_keyword('NAXIS2')

        c = [naxis1/2, naxis2/2]

        wsize = 10
        spaflux = []
        for i in range(wsize, int(naxis2)-wsize):
            spaflux.append(np.median(image[i, int(c[1])-wsize:int(c[1])+wsize]))

        maxflux = np.max(spaflux)
        minflux = np.min(spaflux)

        sig2nois = np.fix(np.sqrt(np.abs(maxflux - minflux)))
        if np.isnan(sig2nois): sig2nois = 'null'

        self.set_keyword('SIG2NOIS', sig2nois, 'KOA: S/N estimate near image spectral center')

        return True


    def run_drp(self):
        '''
        Run the NIRC2 DRP
        '''

        drp = self.config[self.instr]['DRP']
        if os.path.isfile(drp):
            drp = f"{drp} {self.dirs['output']}"
            print(drp)

            cmd = []
            for word in drp.split(' '):
                cmd.append(word)

            log.info(f'run_drp: Running DRP command: {" ".join(cmd)}')
            p = subprocess.Popen(cmd)
            p.wait()
            log.info('run_drp: DRP finished')

        return True


    def run_psfr(self):
        '''
        Starts psfr process that runs parallel with DQA
        '''

        try:
            psfr = self.config[self.instr]['PSFR']
        except:
            log.error('run_psfr: PSFR config item is not defined')
            return False

        cmd = []
        for word in psfr.split(' '):
            cmd.append(word)
        cmd.append(self.instr)
        cmd.append(self.utdate)
        host = gethostname()
        cmd.append(f"/net/{host}{self.dirs['lev0']}")

        log.info(f'run_psfr: Starting PSFR command: {" ".join(cmd)}')
        p = subprocess.Popen(cmd)

        return True

