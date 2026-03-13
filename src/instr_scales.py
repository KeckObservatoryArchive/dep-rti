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
#import pdb


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
            {'name':'set_ofName',      'crit': True},
            {'name':'set_koaimtyp',    'crit': True},
/bin/bash: :q: command not found
            {'name':'set_prog_info',   'crit': True},
            {'name':'set_propint',     'crit': True},
            {'name':'set_elaptime',    'crit': False},
            {'name':'set_datlevel',    'crit': False,  'args': {'level':0}},
            #{'name':'set_filter',      'crit': False}, # need this, but awaiting info
            #{'name':'set_wavelengths', 'crit': False}, # need this, but awaiting info
            {'name':'set_image_stats', 'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
#            {'name':'set_npixsat',     'crit': False,  'args': {'satVal':65535}}, # need SATURATE header kwds; remove?
            #{'name':'set_slitdims',    'crit': False}, # camera='fpc' but need 'fcs'
            #{'name':'set_wcs',         'crit': False}, # not writing values to kwd, no camera!='fpc', but need 'fcs' 
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
        prefix = ''
        if instr == 'scales':
            camera = self.get_keyword('CAMERA', default='').lower()
            allowed = {'ifs':'SS', 'im':'SI', 'dichroic':'SD'}
            prefix = allowed.get(camera, '')
            
        return prefix


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
        if koaimtyp == 'undefined':
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")

        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type from IMTYPE')
        
        return True

        
    def get_koaimtyp(self):
        '''
        Sets koaimtyp based on keyword values
        '''

        allowed = ('object', 'bias', 'dark', 'flatlamp', 'flatlens', 'wavecal')

        imtype = self.get_keyword('IMTYPE', default='undefined').lower()

        if imtype not in allowed:
            return 'undefined'

        return imtype


    def set_elaptime(self):
        '''
        Fixes missing ELAPTIME keyword.
        '''

        if self.get_keyword('ELAPTIME') is None:
            log.info('set_elaptime: ELAPTIME keyword missing, attempting to set from other keywords')
            exptime = self.get_keyword('EXPTIME')
            if exptime is not None:
                self.set_keyword('ELAPTIME', exptime, 'KOA: Total integration time set from EXPTIME')
                log.info('set_elaptime: Setting ELAPTIME from EXPTIME')
                return True
            else:
                self.log_warn('SET_ELAPTIME_ERROR')
                return False

        return True


    # added - what if no FILTER keyword? - changed to instr_nirspec
    def set_filter(self):
        '''
        If FILTER keyword doesn't exist, create from SCIFILT1 and 2
              (Also referred to as FILTER0 and FILTER1)
        NOTE: Filter Wheel Keywords IFSFW[1|2], IMGFW[1|2]
                  or IFS-FW-[1|2], IM-FW-[1|2]
              IFSFWNAM and IMGFWNAM shows the combination position for the imager
        '''
        if self.get_keyword('FILTER', False) != None: return True

        scifilt1 = self.get_keyword('SCIFILT1', default='')
        scifilt2 = self.get_keyword('SCIFILT2', default='')

        skip = ['thick', 'thin', 'open']           # need this?
        if scifilt1.lower() in skip: scifilt1 = ''
        if scifilt2.lower() in skip: scifilt2 = ''

        if scifilt1 == '' and scifilt2 == '':
            filterName = 'blank'
        else:
            filterName = '+'.join(filter(None,(scifilt1, scifilt2)))    # from nirspec
            #filter = ''.join((scifilt1, scifilt2))                     # from kcwi

        #update keyword
        self.set_keyword('FILTER', filterName, 'KOA: set from SCIFILT1 and SCIFILT2')
        #self.set_keyword('FILTER', filter, 'KOA: set from SCIFILT1 and SCIFILT2')
        return True


    # added - changed to instr_nirspec
    def set_wavelengths(self):
        '''
        Sets WAVEMIN and WAVEMAX (in microns) based on FILTER value
            OBSMODE (for CAMERA vaue): imgager, ifs, dichroic
                IMAGER (IM[G]*) - 12.2 x 12.2" FOV (0.006 x 0.006" pixels)
                    - Broad-Band Filters (BBFIL*)
                    - Narrow-Band Filters (NBFIL*)
                    - Neutral-Density Filters (NDFIL*)
                INTEGRAL FIELD SPECTROGRPH (IFS) - 2 X 2" FOV, varies with prism (0.02 x 0.02" spaxels)
                    IFSMODSEL = low-res, med-res (represents spatial size for reduced cubes for IFS) 
                        - Low-Resolution Prisms (LRPRS*)
                        - Medium-Resolution Gratings (MRGRA*)
                DICHROIC (DI*) - captures both imager and spectroscopy data simultaneously (tbd)
        NOTES:
        - Keyword Widths: 6 to 10 chars (verify)
        - Infrared, so blue->min and red->max
        '''
        filters = {}

        # IMAGER Broad-Band Filters
        filters['BBFILY']      = {'min':0.970, 'max':1.070}   # Y
        filters['BBFILJ']      = {'min':1.170, 'max':1.330}   # J
        filters['BBFILH']      = {'min':1.490, 'max':1.780}   # H
        filters['BBFILCH4S']   = {'min':1.530, 'max':1.660}   # CH4s
        filters['BBFILKP']     = {'min':1.950, 'max':2.290}   # Kp
        filters['BBFILKS']     = {'min':1.990, 'max':2.300}   # Ks
        filters['BBFILK']      = {'min':2.030, 'max':2.360}   # K
        filters['BBFILLP']     = {'min':3.430, 'max':4.130}   # Lp
        filters['BBFILMS']     = {'min':4.550, 'max':4.790}   # Ms

        # IMAGER Narrow-Band Filters
        filters['NBFILPABETA'] = {'min':1.280, 'max':1.300}   # Pa-Beta
        filters['NBFILFELL']   = {'min':1.630, 'max':1.660}   # Fell
        filters['NBFILBRGAM']  = {'min':2.150, 'max':2.190}   # Br-Gam
        filters['NBFILKCONT']  = {'min':2.260, 'max':2.290}   # K_cont

        # IMAGER Neutral-Density Filters (in combo with Broad-Band)
        filters['NDFILND1']    = {'min':0.000, 'max':0.000}   # ND1 10x suppression
        filters['NDFILND2']    = {'min':0.000, 'max':0.000}   # ND2 100x suppression
        filters['NDFILND3']    = {'min':0.000, 'max':0.000}   # ND3 1000x suppression

        # IFS Low-Resolution Prisms
        filters['LRPRSK']      = {'min':2.000, 'max':2.400}   # K      (R~150)
        filters['LRPRSKL']     = {'min':2.000, 'max':4.000}   # KL     (R~50) 
        filters['LRPRSKLM']    = {'min':2.000, 'max':5.000}   # KLM    (R~35)
        filters['LRPRSL']      = {'min':2.900, 'max':4.150}   # L      (R~80)
        filters['LRPRSLS']     = {'min':3.100, 'max':3.500}   # LS     (R~200)
        filters['LRPRSM']      = {'min':4.500, 'max':5.200}   # M      (R~200)
        filters['LRPRSKLPOL']  = {'min':2.000, 'max':4.000}   # KL-pol (R~20)

        # IFS Medium-Resolution Gratings
        filters['MRGRAK']      = {'min':2.000, 'max':2.400}   # K      (R~6,000)
        filters['MRGRAL']      = {'min':2.900, 'max':4.150}   # L      (R~2,500)
        filters['MRGRAM']      = {'min':4.500, 'max':5.200}   # M      (R~7,000)

        # FILTER[0,1] values may not be available, so CAMNAME is provided as the 
        # default filter source to be overwritten when FILTERs are specified

        filterName = ''
        filterSource = ''

        camname = self.get_keyword('CAMNAME', default='').upper()
        if camname in filters.keys():
            #filterSource = camname
            filterName = camname

        filterList = self.get_keyword('FILTER', default='').upper().split('+')

        for fitem in filterList:
            if fitem in filters.keys():
                filterName = fitem

        if filterName in filters.keys():
            filterSource = filterName

        # set wavelengths
        wavemin = wavemax = 'null'
        for filt, waves in filters.items():
            if filt in filterSource.upper():
                wavemin = waves['min']
                wavemax  = waves['max']
                break

        self.set_keyword('WAVEMIN', wavemin, 'KOA: Approximate min wavelength (in microns)')
        self.set_keyword('WAVEMAX', wavemax, 'KOA: Approximate max wavelength (in microns)')

        return True


    def set_slitdims(self):
        '''
        Set slit dimensions and wavelengths
        NOTE: will need subpixel region size x and y, infrared detector keywords like
              - SAMPMODE 
              - NREAD
              - NRESET
              - NGROUP
              - NDROP
              - others
        NOTE: No CAMERA keyword, use OBSMODE to get camera value
              IFSMODSEL low-res, med-res (represents spatial size for reduced cubes for IFS)
        '''
        wavemin = 'null'
        #wavecntr = 'null'
        wavemax  = 'null'
        specres  = 'null'
        dispscal = 'null'
        #slitwidt = 'null'
        #slitlen  = 'null'
        spatscal = 'null'

        #slicer = self.get_keyword('IFUNAM').lower()     # remove
        camera = self.get_keyword('CAMERA')
        # binning = self.get_keyword('BINNING')          # remove
        # lowercase camera if not None
        camera = camera.lower() if camera is not None else camera

        prefix = "R" if camera=="red" else "B"
        #cwave = self.get_keyword(prefix+'CWAVE', default=0)     # remove?
        #gratname = self.get_keyword(prefix+'GRATNAM').lower()   # remove?
        #nodmask = self.get_keyword(prefix+'NASNAM').lower()     # remove?
        # confirm configuration for SCALES?
        configurations = {
                          'bl'  : {'waves':2000, 'large':900, 'medium':1800, 'small':3600},
                          }
        
        # slit width by slicer, slit length is always 20.4"
        #slits = {'large':'1.35', 'medium':'0.69', 'small':'0.35'}
        #if slicer in slits.keys():
        #    slitwidt = slits[slicer]
        #    slitlen = 108

        # get wavelengths from configuration dictionary
        if gratname in configurations.keys() and slicer in slits.keys():                 # remove?
            if cwave > 0:                                                                # remove?
                #wavecntr = round(cwave)
                #wavemin = round(wavecntr - configurations.get(gratname)['waves']/2)     # remove?
                #wavemax   = round(wavecntr + configurations.get(gratname)['waves']/2)   # remove?
            #specres = configurations.get(gratname)[slicer]                              # remove?
            if nodmask == "mask":                                                        # remove?
                diff = int((wavemax - wavemin)/3)
                diff = int(math.ceil(diff/100.0)*100)
                wavemin = wavecntr - diff                                               # remove?
                wavemax = wavecntr + diff                                               # remove? 
        
        # camera plate scale, arcsec/pixel unbinned
        #TODO verify pscale for red, svc
        pscale = {'imager':0.06, 'small':0.02, 'medium': 0.02}
        if camera in pscale.keys():
            try:
                dispscal = pscale.get(camera) * binning                                 # remove?
            except:
                dispscal = pscale.get(camera) * int(binning.split(',')[0])              # remove?
            spatscal = dispscal
            if camera == 'fpc':
                wavemin = 3700
                #wavecntr = 6850
                wavemax = 10000
        
        #try:
        #    slitwidt = float(slitwidt)
        #except:
        #    pass

        #set slit dimensions and wavelengths
        self.set_keyword('WAVEMIN',waveblue,'KOA: Min wavelength')
        self.set_keyword('WAVEMAX',wavered,'KOA: Max wavelength')
        self.set_keyword('SPECRES',specres,'KOA: Nominal spectral resolution')
        self.set_keyword('SPATSCAL',spatscal,'KOA: CCD pixel scale, spatial')
        self.set_keyword('DISPSCAL',dispscal,'KOA: CCD pixel scale, dispersion')
        #self.set_keyword('SLITWIDT',slitwidt,'KOA: Slit width on sky')          # remove n/a
        #self.set_keyword('SLITLEN',slitlen,'KOA: Slit length on sky')           # remove n/a

        # IFSMODSEL low-res, med-res (represents spatial size for reduced cubes for IFS)

        return True


    def set_wcs(self):
        '''
        Set world coordinate system values
        NOTE: no fpc, but will need wcs/fcs for the imager and the cubified IFS data, 
              and maybe bad/bizarre version for the basic IFS data
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
        #binning = self.get_keyword('BINNING')                # remove?
        #self.set_keyword('BINNING',str(binning),'Binning: serial/axis1, parallel/axis2')     # remove?
        # special PA calculation determined by rotmode
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

        # get correct units and formatting
        raindeg = 1
        # pixscale = 0.0075 * float(binning)                   # remove?
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
