'''
This is the class to handle all the KCWI specific attributes
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
import logging
log = logging.getLogger('koa_dep')


class Kcwi(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):

        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        self.keymap['UTC'] = 'UT'


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',       'crit': True},
            {'name':'set_ut',          'crit': True},
            {'name':'set_telescope',   'crit': False},
            {'name':'set_ofName',      'crit': True},
            {'name':'set_koaimtyp',    'crit': True},
            {'name':'set_frameno',     'crit': True},
            {'name':'set_semester',    'crit': True},
            {'name':'set_prog_info',   'crit': True},
            {'name':'set_propint',     'crit': True},
            {'name':'set_elaptime',    'crit': False},
            {'name':'set_datlevel',    'crit': False,  'args': {'level':0}},
            {'name':'set_image_stats', 'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
            {'name':'set_npixsat',     'crit': False,  'args': {'satVal':65535}},
            {'name':'set_slitdims',    'crit': False},
            {'name':'set_wcs',         'crit': False},
            {'name':'set_dqa_vers',    'crit': False},
            {'name':'set_dqa_date',    'crit': False},
        ]
        return self.run_functions(funcs)


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
                elif camera == 'svc':
                    prefix = 'KS'
                else:
                    prefix = ''
            except:
                prefix = ''
        else:
            prefix = ''
        return prefix


    def set_telescope(self):
        '''
        Set telescope to Keck 2
        '''
        if self.get_keyword('TELESCOP') != None: return True
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
            self.log_warn("KOAIMTYP_UDF")

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
        elif camera == 'svc':
            koaimtyp = 'svc'
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
            self.log_warn('SET_ELAPTIME_ERROR')
            return False
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')
        return True

    def set_slitdims(self):
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
        binning = self.get_keyword('BINNING')
        #lowercase camera if not None
        camera = camera.lower() if camera is not None else camera

        prefix = "R" if camera=="red" else "B"
        cwave = self.get_keyword(prefix+'CWAVE', default=0)
        gratname = self.get_keyword(prefix+'GRATNAM').lower()
        nodmask = self.get_keyword(prefix+'NASNAM').lower()
        # Configuration for KB
        configurations = {
                          'bl'  : {'waves':2000, 'large':900, 'medium':1800, 'small':3600},
                          'bm'  : {'waves':850, 'large':2000, 'medium':4000, 'small':8000},
                          'bh3' : {'waves':500, 'large':4500, 'medium':9000, 'small':18000},
                          'bh2' : {'waves':405, 'large':4500, 'medium':9000, 'small':18000},
                          'bh1' : {'waves':400, 'large':4500, 'medium':9000, 'small':18000},
                          #TODO: verify these numbers are accurate.
                          'rl'  : {'waves':275, 'large': 10500, 'medium':7000, 'small':3500},
                          'rm1' : {'waves':145, 'large':4050, 'medium':2700, 'small':1350},
                          'rm2' : {'waves':190, 'large':4700, 'medium':2800, 'small':1900},
                          'rh1' : {'waves':60, 'large':1800, 'medium':1200, 'small':600},
                          'rh2' : {'waves':75, 'large':2025, 'medium':1350, 'small':675},
                          'rh3' : {'waves':90, 'large':2400, 'medium':1600, 'small':800},
                          'rh4' : {'waves':80, 'large':2775, 'medium':1850, 'small':925},
                          }
        
        # Slit width by slicer, slit length is always 20.4"
        slits = {'large':'1.35', 'medium':'0.69', 'small':'0.35'}
        if slicer in slits.keys():
            slitwidt = slits[slicer]
            slitlen = 20.4
        #get wavelengths from configuration dictionary
        if gratname in configurations.keys() and slicer in slits.keys():
            if cwave > 0:
                wavecntr = round(cwave)
                waveblue = round(wavecntr - configurations.get(gratname)['waves']/2)
                wavered  = round(wavecntr + configurations.get(gratname)['waves']/2)
            specres = configurations.get(gratname)[slicer]
            if nodmask == "mask":
                diff = int((wavered - waveblue)/3)
                diff = int(math.ceil(diff/100.0)*100)
                waveblue = wavecntr - diff
                wavered = wavecntr + diff
        
        # Camera plate scale, arcsec/pixel unbinned
        #TODO verify pscale for red, svc
        pscale = {'fpc':0.0075, 'blue':0.147, 'red': 0.147}
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
            self.log_warn("SET_WCS_ERROR", mode)
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


    def has_target_info(self):
        '''
        Does this fits have sensitive target info?
        '''
        return False


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP specific to KCWI.

        Raw ingest (KOA level 1)
            icubed.fits files
            icubes.fits files
            calibration validation (arc_ and bars_ < FRAMENO)

        Final ingest (KOA level 2)
            icubes.fits or icubed.fits (if no flux standard)           
            calibration validation (sky_ and scat_ == FRAMENO)
            QA (all plots in plots directory from pipeline)
            kcwi.proc
            all logs
            configuration file
        '''
        files = []

        #back out of /redux/ subdir
#        if level == 1:
        if datadir.endswith('/'): datadir = datadir[:-1]
        datadir = os.path.split(datadir)[0]

        #get frameno
        hdr = None
        icubed = f"{datadir}/redux/{koaid}_icubed.fits"
        icubes = f"{datadir}/redux/{koaid}_icubes.fits"
        if os.path.isfile(icubed):
            hdr = fits.getheader(icubed)
        elif os.path.isfile(icubes):
            hdr = fits.getheader(icubes)
        if not hdr:
            return False
        frameno = hdr['FRAMENO']

        #level 1
        if level >= 1:
            searchfiles = [
                f"{datadir}/redux/{koaid}_icubed.fits",
                f"{datadir}/redux/{koaid}_icubes.fits"
            ]
            for f in searchfiles:
                if os.path.isfile(f): files.append(f)
            for file in glob.glob(f"{datadir}/plots/*"):
                fparts = os.path.basename(file).split('_')
                if fparts[0] not in ('arc', 'bars', 'bias','ql'): continue
                if not fparts[1].isdigit(): continue
                if int(fparts[1]) >= frameno: continue
                files.append(file)

        #level 2 (note: includes level 1 stuff, see above)
        if level == 2:
            searchfiles = [
                f"{datadir}/kcwi.proc",
                f"/k2drpdata/KCWI_DRP/configs/kcwi_koarti_lev2.cfg"
            ]
            for f in searchfiles:
                if os.path.isfile(f): files.append(f)
            for file in glob.glob(f"{datadir}/plots/*"):
                fparts = os.path.basename(file).split('_')
                if fparts[0] not in ('sky', 'scat', 'std'): continue
                if not fparts[1].isdigit(): continue
                if int(fparts[1]) != frameno: continue
                files.append(file)
            for file in glob.glob(f"{datadir}/logs/*"):
                files.append(file)

        return files


    def get_unique_koaids_in_dir(self, datadir):
        '''
        Get a list of unique koaids by looking at all filenames in directory 
        and regex matching a KOAID pattern.
        '''
        koaids = []
        for path in Path(datadir).rglob('*'):
            path = str(path)
            fname = os.path.basename(path)
            if not any(x in fname for x in ('_icubes', '_icubed')): continue
            match = re.search(r'^(\D{2}\.\d{8}\.\d{5}\.\d{2})', fname)
            if not match: continue
            koaids.append(match.groups(1)[0])
        koaids = list(set(koaids))
        return koaids


    def create_ext_meta(self):
        '''Override parent function'''
        return True
