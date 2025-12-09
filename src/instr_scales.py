'''
This is the class to handle all the SCALES specific attributes
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
from astropy.visualization import ZScaleInterval, SqrtStretch
#from astropy.visualization import ZScaleInterval, AsinhStretch, SinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize


class Scales(instrument.Instrument):

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
#            {'name':'set_frameno',     'crit': True},
            {'name':'set_semester',    'crit': True},
            {'name':'set_prog_info',   'crit': True},
            {'name':'set_propint',     'crit': True},
            {'name':'set_elaptime',    'crit': False},
            {'name':'set_datlevel',    'crit': False,  'args': {'level':0}},
            {'name':'set_image_stats', 'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
#            {'name':'set_npixsat',     'crit': False,  'args': {'satVal':65535}}, # need SATURATE header kwds
            #{'name':'set_slitdims',    'crit': False}, # need headerheader kwds: IFUNAM CWAVE GRATNAM NASNAM, camera!='fpc'
            #{'name':'set_wcs',         'crit': False}, # not writing values to kwd headers camera!='fpc' so n/a?
            {'name':'set_dqa_vers',    'crit': False},
            {'name':'set_dqa_date',    'crit': False},
        ]
        return self.run_functions(funcs)


    def get_dir_list(self):
        '''
        Function to generate the paths to all the SCALES accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata1800/scales'
        for i in range(1,10):
            joinSeq = (path, str(i))
            path2 = ''.join(joinSeq)
            dirs.append(path2)

        # handle the utility accounts
        path = '/s/sdata1800/sca'
        joinSeq = (path, 'dev')
        path2 = ''.join(joinSeq)
        dirs.append(path2)
        joinSeq = (path, 'eng')
        path2 = ''.join(joinSeq)
        dirs.append(path2)
        return dirs


    def get_prefix(self):
        instr = self.get_instr()
        if instr == 'scales':
            try:
                camera = self.get_keyword('OBSMODE').lower()
                if camera in ['low-res', 'med-res', 'ifs']:
                    prefix = 'SS'
                elif camera in ('imager'):
                    prefix = 'SI'
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


    def create_jpg_from_fits(self, fits_filepath, outdir_path):
        '''
        Basic convert fits primary data to jpg. overrides super class function
        '''

        try:
            with fits.open(fits_filepath, ignore_missing_end=True) as hdul:
                data = hdul[0].data
                hdr = hdul[0].header
        except Exception as e:
            logger.warning(f"Problem with FITS file {fits_filepath}: {e}")
            return None

        if data is None:
            raise ValueError(f"No data in primary HDU of {fits_filepath}")
            return None

        # NAXIS dimensionality from header, fall back to ndim if missing
        naxis = hdr.get("NAXIS", data.ndim)

        # FITS convention: NAXIS1 = x, NAXIS2 = y single slice, NAXIS3 = number of slices
        if naxis == 3:     # flattens 3D cube -> 2D "dirty FITS"
            result = data[-1] - data[0]
        elif naxis == 2:   # 2D image regular/original processing
            result = data
        else:   # other dimensionality: first slice along axis 0
            result = data[0]

        # all objects should be 2D for JPEG
        if result.ndim != 2:
            raise ValueError(
                f"Resulting array is {result.ndim}D, expected 2D for JPEG. "
                f"Shape: {result.shape}. Adjust the slicing logic for this file."
            )
    
        # cleans NaNs / infs
        result = np.nan_to_num(result, nan=0.0, posinf=0.0, neginf=0.0)
        result = np.asarray(result, dtype=np.float32)

        # CREATE JPEG
        # 1-scaling and limits
        interval = ZScaleInterval()
        try:
            vmin, vmax = interval.get_limits(result)
        except Exception as e:
            raise ValueError(f"ZScaleInterval failed on data from {fits_path}: {e!r}")
    
        # for nonsense zscale, fall back to min/max
        if (not np.isfinite(vmin)) or (not np.isfinite(vmax)) or (vmax <= vmin):
            vmin, vmax = np.nanmin(result), np.nanmax(result)
    
        # 2a-normalize if min/max , give up and make a flat gray image
        if (not np.isfinite(vmin)) or (not np.isfinite(vmax)) or (vmax <= vmin):
            norm_arr = np.zeros_like(result, dtype=np.float32)
        else:
            # swap SqrtStretch() or SinhStretch() for AsinhStretch() if preferred
            norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=SqrtStretch())
            #norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
            #norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=SinhStretch())  # may sinh error
            norm_arr = scaled = norm(result)    # float array in [0, 1]   # norm_arr
    
        # 2b- stretch ~0...1 ensures values are in [0, 1]
        norm_arr = np.clip(norm_arr, 0.0, 1.0)
    
        # 3-optional forensic mode: histogram equalization on normalized data to increase contrast
        #image_eq = exposure.equalize_hist(scaled)

        # builds output filepath
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        jpg_filepath = f'{outdir_path}/{basename}.jpg'

        # option 1: saves faster version, no fig necessary
        # image_eq or norm_arr is final 0-1 float array
        #final_img = image_eq 
        final_img = norm_arr 
        plt.imsave(jpg_filepath, final_img, cmap="gray", format="jpg")


    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm, Calls get_koaimtyp for algorithm
        '''

        koaimtyp = self.get_koaimtyp()
        
        # warn if undefined
        #if (koaimtyp == 'undefined'):
        if not koaimtyp:
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")
            koaimtyp = 'undefined'

        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type from IMTYPE')
        
        return True

        
    def get_koaimtyp(self):
        '''
        Sets koaimtyp based on keyword values
        '''
        # missing: 'bad', 'contbars', 'focus'
        allowed = ('object', 'bias', 'dark', 'arclamp', 'flatlamp',
                   'domeflat', 'twiflat', 'undefined')

        koaimtyp = 'undefined'
        imtype = self.get_keyword('IMTYPE')
        if not imtype:
            return 'undefined'

        imtype = imtype.lower()
        if imtype in allowed:
            return imtype

        return 'undefined'


    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword.
        '''
        itime  = self.get_keyword('TRUITIME')
        if self.get_keyword('ELAPTIME') is not None:
            elaptime = self.get_keyword('ELAPTIME')
        elif self.get_keyword('EXPTIME') is not None:
            elaptime = self.get_keyword('EXPTIME')
            log.info('set_elaptime: Setting ELAPTIME from EXPTIME')
        elif self.get_keyword('XPOSURE') is not None:
            elaptime = self.get_keyword('XPOSURE')
            log.info('set_elaptime: Setting ELAPTIME from XPOSURE')
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
        # lowercase camera if not None
        camera = camera.lower() if camera is not None else camera

        prefix = "R" if camera=="red" else "B"
        cwave = self.get_keyword(prefix+'CWAVE', default=0)
        gratname = self.get_keyword(prefix+'GRATNAM').lower()
        nodmask = self.get_keyword(prefix+'NASNAM').lower()
        # confirm configuration for SCALES?
        configurations = {
                          'bl'  : {'waves':2000, 'large':900, 'medium':1800, 'small':3600},
                          }
        
        # slit width by slicer, slit length is always 20.4"
        slits = {'large':'1.35', 'medium':'0.69', 'small':'0.35'}
        if slicer in slits.keys():
            slitwidt = slits[slicer]
            slitlen = 108
        # get wavelengths from configuration dictionary
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
        
        # camera plate scale, arcsec/pixel unbinned
        #TODO verify pscale for red, svc
        pscale = {'imager':0.06, 'small':0.02, 'medium': 0.02}
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
        # extract values from header
        camera = self.get_keyword('CAMERA')
        # wcs values should only be set for fpc
        if camera != 'fpc':
            log.info(f'set_wcs: WCS keywords not set for camera type: {camera}')
            return True
        # get ra and dec values
        rakey = (self.get_keyword('RA')).split(':')
        rakey = 15.0*(float(rakey[0])+(float(rakey[1])/60.0)+(float(rakey[2])/3600.0))
        deckey = (self.get_keyword('DEC')).split(':')
        # compensation for negative dec if applicable
        if float(deckey[0]) < 0:
            deckey = float(deckey[0])-(float(deckey[1])/60.0)-(float(deckey[2])/3600.0)
        else:
            deckey = float(deckey[0])+(float(deckey[1])/60.0)+(float(deckey[2])/3600.0)
        
        # get more keywords
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
        # special PA calculation determined by rotmode
        # pa = rotposn + parantel - el
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

        # get correct units and formatting
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


#    def make_jpg(self):
#        # Skip if this an image cube
#        if self.isImageCube:
#            return True
#        return super().make_jpg()


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP specific to SCALES.

        Raw ingest (KOA level 1)
            icubed.fits files
            icubes.fits files
            calibration validation (arc_ and bars_ < FRAMENO)

        Final ingest (KOA level 2)
            icubes.fits or icubed.fits (if no flux standard)           
            calibration validation (sky_ and scat_ == FRAMENO)
            QA (all plots in plots directory from pipeline)
            scales.proc
            all logs
            configuration file
        '''
        files = []

        # back out of /redux/ subdir
        #if level == 1:
        if datadir.endswith('/'): datadir = datadir[:-1]
        datadir = os.path.split(datadir)[0]

        # get frameno
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

        # level 1
        if level >= 1:
            searchfiles = [
                f"{datadir}/redux/{koaid}_rampfit.fits",
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

        # level 2 (note: includes level 1 stuff, see above)
        if level == 2:
            searchfiles = [
                f"{datadir}/scales.proc",
                f"/k2drpdata/SCALES_DRP/configs/scales_koarti_lev2.cfg"
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
