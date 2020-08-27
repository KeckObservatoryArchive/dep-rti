'''
This is the class to handle all the KCWI specific attributes
KCWI specific DR techniques can be added to it in the future

01/31/2020 E. Lucas - Updated functions
12/14/2017 M. Brown - Created initial file
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

class Kcwi(instrument.Instrument):
    def __init__(self, instr, utDate, rootdir, log=None):
        # Call the parent init to get all the shared variables
        super().__init__(instr, utDate, rootdir, log)

        # Other vars that subclass can overwrite
        self.endTime = '20:00:00'   # 24 hour period start/end time (UT)
        self.sdataList = self.get_dir_list()
        self.keymap['UTC'] = 'UT'

    def get_dir_list(self):
        '''
        Function to generate the paths to all the KCWI accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata1400/kcwi'
        for i in range(1,10):
            joinSeq = (path, str(i))
            path2 = ''.join(joinSeq)
            dirs.append(path2)
        joinSeq = (path, 'dev')
        path2 = ''.join(joinSeq)
        dirs.append(path2)
        return dirs

    def get_prefix(self):
        instr = self.get_instr()
        if instr == 'kcwi':
            try:
                camera = self.get_keyword('CAMERA').lower()
                if camera == 'blue':
                    prefix = 'KB'
                elif camera == 'red':
                    prefix = 'KR'
                elif camera == 'fpc':
                    prefix = 'KF'
                else:
                    prefix = ''
            except:
                prefix = ''
        else:
            prefix = ''
        return prefix

    def run_dqa(self, progData):
        '''
        Run all DQA checks unique to this instrument.
        '''
        ok = True
        if ok: ok = self.set_telescope()
        if ok: ok = self.set_filename()
        if ok: ok = self.set_elaptime()
        if ok: ok = self.set_koaimtyp()
        if ok: ok = self.set_frameno()
        if ok: ok = self.set_semester()
        if ok: ok = self.set_prog_info(progData)
        if ok: ok = self.set_propint(progData)
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.set_image_stats_keywords()
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_npixsat(satVal=65535)
        if ok: ok = self.set_slitdims_wavelengths()
        if ok: ok = self.set_wcs()
        if ok: ok = self.set_dqa_vers()
        if ok: ok = self.set_dqa_date()
        return ok
    
    def set_filename(self):
        '''
        Map OFNAME
        '''
        self.keymap['OFNAME'] = 'OFNAME'
        return True

    def set_telescope(self):
        '''
        Set telescope to Keck 2
        '''
        self.set_keyword('TELESCOP','Keck II','KOA: Telescope name')
        return True


    def create_jpg_from_fits(self, fits_filepath, outdir):
        '''
        Basic convert fits primary data to jpg.  Instrument subclasses can override this function.
        '''

        #get image data
        hdu = fits.open(fits_filepath, ignore_missing_end=True)
        data = hdu[0].data
        hdr  = hdu[0].header
        #use histogram equalization to increase contrast
        image_eq = exposure.equalize_hist(data)
        
        #form filepaths
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        jpg_filepath = f'{outdir}/{basename}.jpg'
        #create jpg
        dpi = 100
        width_inches  = hdr['NAXIS1'] / dpi
        height_inches = hdr['NAXIS2'] / dpi
        fig = plt.figure(figsize=(width_inches, height_inches), frameon=False, dpi=dpi)
        ax = fig.add_axes([0, 0, 1, 1]) #this forces no border padding
        plt.axis('off')
        plt.imshow(image_eq, cmap='gray', origin='lower')#, norm=norm)
        plt.savefig(jpg_filepath, quality=92)
        plt.close()

    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm
        Calls get_koaimtyp for algorithm
        '''

        koaimtyp = self.get_koaimtyp()
        
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

    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword.
        '''
        itime  = self.get_keyword('ITIME')
        coadds = self.get_keyword('COADDS')
        #use elaptime if set, otherwise check other keywords
        if self.get_keyword('ELAPTIME') is not None:
            elaptime = self.get_keyword('ELAPTIME')
        elif self.get_keyword('EXPTIME') is not None:
            elaptime = self.get_keyword('EXPTIME')
            log.info('set_elaptime: Setting ELAPTIME from EXPTIME')
        elif self.get_keyword('XPOSURE') is not None:
            elaptime = self.get_keyword('XPOSURE')
            log.info('set_elaptime: Setting ELAPTIME from XPOSURE')
        elif itime != None and coadds != None:
            elaptime = round(itime*coadds,4)
            log.info('set_elaptime: Setting ELAPTIME from ITIME*COADDS')
        else:
            elaptime = ''
            log.warning('set_elaptime: no methods possible for setting elaptime')
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        return True

    def set_slitdims_wavelengths(self):
        '''
        Set slit dimensions and wavelengths
        '''
        waveblue = 'null'
        wavecntr = 'null'
        wavered  = 'null'
        specres  = 'null'
        dispscal = 'null'
        slitwidt = 'null'
        slitlen  = 'null'
        spatscal = 'null'

        slicer = self.get_keyword('IFUNAM').lower()
        camera = self.get_keyword('CAMERA')
        #lowercase camera if not None
        try:
            camera = camera.lower()
        except:
            pass
        binning = self.get_keyword('BINNING')
        gratname = self.get_keyword('BGRATNAM').lower()
        nodmask = self.get_keyword('BNASNAM').lower()
        
        # Configuration for KB
        configurations = {'bl' : {'waves':(3500, 4550, 5600), 'large':900, 'medium':1800, 'small':3600},
                  'bm' : {'waves':(3500, 4500, 5500), 'large':2000, 'medium':4000, 'small':8000},
                  'bh3' : {'waves':(4700, 5150, 5600), 'large':4500, 'medium':9000, 'small':18000},
                  'bh2' : {'waves':(4000, 4400, 4800), 'large':4500, 'medium':9000, 'small':18000},
                  'bh1' : {'waves':(3500, 3800, 4100), 'large':4500, 'medium':9000, 'small':18000}}
        
        # Slit width by slicer, slit length is always 20.4"
        slits = {'large':'1.35', 'medium':'0.69', 'small':'0.35'}
        if slicer in slits.keys():
            slitwidt = slits[slicer]
            slitlen = 20.4
        #get wavelengths from configuration dictionary
        if gratname in configurations.keys() and slicer in slits.keys():
            waveblue = configurations.get(gratname)['waves'][0]
            wavecntr = configurations.get(gratname)['waves'][1]
            wavered = configurations.get(gratname)['waves'][2]
            specres = configurations.get(gratname)[slicer]
            if nodmask == "mask":
                diff = int((wavered - waveblue)/3)
                diff = int(math.ceil(diff/100.0)*100)
                waveblue = wavecntr - diff
                wavered = wavecntr + diff
        
        # Camera plate scale, arcsec/pixel unbinned
        pscale = {'fpc':0.0075, 'blue':0.147}
        if camera in pscale.keys():
            try:
                dispscal = pscale.get(camera) * binning
            except:
                dispscal = pscale.get(camera) * int(binning.split(',')[0])
            spatscal = dispscal
            if camera == 'fpc':
                waveblue = 3700
                wavecntr = 6850
                wavered = 10000
        
        try:
            slitwidt = float(slitwidt)
        except:
            pass
        #set slit dimensions and wavelengths
        self.set_keyword('WAVEBLUE',waveblue,'KOA: Blue end wavelength')
        self.set_keyword('WAVECNTR',wavecntr,'KOA: Central wavelength')
        self.set_keyword('WAVERED',wavered,'KOA: Red end wavelength')
        self.set_keyword('SPECRES',specres,'KOA: Nominal spectral resolution')
        self.set_keyword('SPATSCAL',spatscal,'KOA: CCD pixel scale, spatial')
        self.set_keyword('DISPSCAL',dispscal,'KOA: CCD pixel scale, dispersion')
        self.set_keyword('SLITWIDT',slitwidt,'KOA: Slit width on sky')
        self.set_keyword('SLITLEN',slitlen,'KOA: Slit length on sky')

        return True


    def set_wcs(self):
        '''
        Set world coordinate system values
        '''
        #extract values from header
        camera = self.get_keyword('CAMERA')
        #wcs values should only be set for fpc
        if camera != 'fpc':
            log.info(f'set_wcs: WCS keywords not set for camera type: {camera}')
            return True
        #get ra and dec values
        rakey = (self.get_keyword('RA')).split(':')
        rakey = 15.0*(float(rakey[0])+(float(rakey[1])/60.0)+(float(rakey[2])/3600.0))
        deckey = (self.get_keyword('DEC')).split(':')
        #compensation for negative dec if applicable
        if float(deckey[0]) < 0:
            deckey = float(deckey[0])-(float(deckey[1])/60.0)-(float(deckey[2])/3600.0)
        else:
            deckey = float(deckey[0])+(float(deckey[1])/60.0)+(float(deckey[2])/3600.0)
        
        #get more keywords
        equinox = self.get_keyword('EQUINOX')
        naxis1 = self.get_keyword('NAXIS1')
        naxis2 = self.get_keyword('NAXIS2')
        pa = self.get_keyword('ROTPOSN')
        rotmode = self.get_keyword('ROTMODE')
        parantel = self.get_keyword('PARANTEL')
        parang = self.get_keyword('PARANG')
        el = self.get_keyword('EL')
        binning = self.get_keyword('BINNING')
        self.set_keyword('BINNING',str(binning),'Binning: serial/axis1, parallel/axis2')
        #special PA calculation determined by rotmode
        #pa = rotposn + parantel - el
        mode = rotmode[0:4]
        if parantel == '' or parantel == None:
            parantel = parang
        if mode == 'posi':
            pa1 = float(pa)
        elif mode == 'vert':
            pa1 = float(pa)+float(parantel)
        elif mode == 'stat':
            pa1 = float(pa)+float(parantel)-float(el)
        else:
            log.error(f'set_wcs: indeterminate mode {mode}')
            return False

        #get correct units and formatting
        raindeg = 1
        pixscale = 0.0075 * float(binning)
        pa0 = 0.7
        crval1 = rakey
        crval2 = deckey

        pa = -(pa1 - pa0)*np.pi/180.0
        cd1_1 = -pixscale*np.cos(pa)/3600.0
        cd2_2 = pixscale*np.cos(pa)/3600.0
        cd1_2 = -pixscale*np.sin(pa)/3600.0
        cd2_1 = -pixscale*np.sin(pa)/3600.0

        cd1_1 = '%18.7e' % cd1_1
        cd2_2 = '%18.7e' % cd2_2
        cd1_2 = '%18.7e' % cd1_2
        cd2_1 = '%18.7e' % cd2_1

        pixscale = '%8.6f' % pixscale
        crpix1 = (float(naxis1)+1.0)/2.0
        crpix2 = (float(naxis2)+1.0)/2.0
        crpix1 = '%8.2f' % crpix1
        crpix2 = '%8.2f' % crpix2

        # check equinox
        if float(equinox) == 2000.0:
            radecsys = 'FK5'
        else:
            radecsys = 'FK4'
        
        #set keywords
        self.set_keyword('CD1_1',float(cd1_1),'KOA: WCS coordinate transformation matrix')
        self.set_keyword('CD1_2',float(cd1_2),'KOA: WCS coordinate transformation matrix')
        self.set_keyword('CD2_1',float(cd2_1),'KOA: WCS coordinate transformation matrix')
        self.set_keyword('CD2_2',float(cd2_2),'KOA: WCS coordinate transformation matrix')
        self.set_keyword('CRPIX1',float(crpix1),'KOA: Reference pixel')
        self.set_keyword('CRPIX2',float(crpix2),'KOA: Reference pixel')
        self.set_keyword('CRVAL1',crval1,'KOA: Reference pixel value')
        self.set_keyword('CRVAL2',crval2,'KOA: Reference pixel value')
        self.set_keyword('RADECSYS',radecsys,'KOA: WCS coordinate system')
        self.set_keyword('CTYPE1','RA---TAN','KOA: WCS type of the horizontal coordinate')
        self.set_keyword('CTYPE2','DEC--TAN','KOA: WCS type of the vertical coordinate')
        
        return True

