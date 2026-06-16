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
        
        # Set filter list
        self.all_filters = {
            "IM" : {
                'Y':      {'min':0.970, 'max':1.070},
                'J':      {'min':1.170, 'max':1.330},
                'H':      {'min':1.490, 'max':1.780},
                'CH4s':   {'min':1.530, 'max':1.660},
                'Kp':     {'min':1.950, 'max':2.290},
                'Ks':     {'min':1.990, 'max':2.300},
                'K':      {'min':2.030, 'max':2.360},
                'Lp':     {'min':3.430, 'max':4.130},
                'Ms':     {'min':4.550, 'max':4.790},
                'PaBeta': {'min':1.280, 'max':1.300},
                'FeII':   {'min':1.630, 'max':1.660},
                'BrGam':  {'min':2.150, 'max':2.190},
                'Kcont':  {'min':2.260, 'max':2.290}
            },
            "IFS" : {
                'K':      {'min':2.000, 'max':2.400},
                'KL':     {'min':2.000, 'max':4.000}, 
                'KLM':    {'min':2.000, 'max':5.000},
                'L':      {'min':2.900, 'max':4.150},
                'Ls':     {'min':3.100, 'max':3.500},
                'M':      {'min':4.500, 'max':5.200},
                'KLpol':  {'min':2.000, 'max':4.000}
            }
        }


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',       'crit': True},
            {'name':'set_ut',          'crit': True},
            {'name':'set_semester',    'crit': True},
            {'name':'set_ofName',      'crit': True},
            {'name':'set_koaimtyp',    'crit': True},
            {'name':'set_prog_info',   'crit': True},
            {'name':'set_propint',     'crit': True},
            {'name':'set_elaptime',    'crit': False},
            {'name':'set_datlevel',    'crit': False,  'args': {'level':0}},
            {'name':'set_wavelengths', 'crit': False}, # need this, but awaiting info
            {'name':'set_image_stats', 'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
            #{'name':'set_slitdims',    'crit': False}, # camera='fpc' but need 'fcs'
            {'name':'set_dqa_vers',    'crit': False},
            {'name':'set_dqa_date',    'crit': False},
        ]
        return self.run_functions(funcs)


    def get_prefix(self):
        '''
        Returns the KOAID prefix
        '''

        instr = self.get_instr()
        prefix = ''
        if instr.lower() == 'scales':
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
            exptime = self.get_keyword('EXPTIME')
            if exptime is not None:
                self.set_keyword('ELAPTIME', exptime, 'KOA: Total integration time set from EXPTIME')
                log.info('set_elaptime: Setting ELAPTIME from EXPTIME')
                return True
            else:
                self.log_warn('SET_ELAPTIME_ERROR')
                return False

        return True


    def set_wavelengths(self):
        '''
        Sets wavelength values based on filter
        '''

        waveblue = wavecntr = wavered = 'null'
        
        # FILTER[0,1] values may not be available, so CAMNAME is provided as the 
        # default filter source to be overwritten when FILTERs are specified

        count = 0
        filterList = []
        camera = self.get_keyword('CAMERA', default='').upper()
        if camera in self.all_filters.keys():
            filters = self.all_filters[camera]
            filterList = self.get_keyword('FILTER', default='').split('+')

            for fitem in filterList:
                if fitem in filters.keys():
                    waveblue = round(filters[fitem]['min'], 2)
                    wavered  = round(filters[fitem]['max'], 2)
                    wavecntr = round((wavered + waveblue)/2, 2)
                    count += 1

        if count == 1:
            self.set_keyword('WAVEBLUE',waveblue,'KOA: Approximate blue end wavelength (microns)')
            self.set_keyword('WAVECNTR',wavecntr,'KOA: Approximate central wavelength (microns)')
            self.set_keyword('WAVERED',wavered,'KOA: Approximate red end wavelength (microns)')
        else:
            self.log_warn(f'set_wavelengths: error setting wavelengths from FILTER={filterList}')

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
#        if gratname in configurations.keys() and slicer in slits.keys():                 # remove?
#            if cwave > 0:                                                                # remove?
#                #wavecntr = round(cwave)
#                #wavemin = round(wavecntr - configurations.get(gratname)['waves']/2)     # remove?
#                #wavemax   = round(wavecntr + configurations.get(gratname)['waves']/2)   # remove?
#            #specres = configurations.get(gratname)[slicer]                              # remove?
#            if nodmask == "mask":                                                        # remove?
#                diff = int((wavemax - wavemin)/3)
#                diff = int(math.ceil(diff/100.0)*100)
#                wavemin = wavecntr - diff                                               # remove?
#                wavemax = wavecntr + diff                                               # remove? 
#        
#        # camera plate scale, arcsec/pixel unbinned
#        #TODO verify pscale for red, svc
#        pscale = {'imager':0.06, 'small':0.02, 'medium': 0.02}
#        if camera in pscale.keys():
#            try:
#                dispscal = pscale.get(camera) * binning                                 # remove?
#            except:
#                dispscal = pscale.get(camera) * int(binning.split(',')[0])              # remove?
#            spatscal = dispscal
#            if camera == 'fpc':
#                wavemin = 3700
#                #wavecntr = 6850
#                wavemax = 10000
#        
#        #try:
#        #    slitwidt = float(slitwidt)
#        #except:
#        #    pass
#
#        #set slit dimensions and wavelengths
#        self.set_keyword('SPECRES',specres,'KOA: Nominal spectral resolution')
#        self.set_keyword('SPATSCAL',spatscal,'KOA: CCD pixel scale, spatial')
#        self.set_keyword('DISPSCAL',dispscal,'KOA: CCD pixel scale, dispersion')
#        #self.set_keyword('SLITWIDT',slitwidt,'KOA: Slit width on sky')          # remove n/a
#        #self.set_keyword('SLITLEN',slitlen,'KOA: Slit length on sky')           # remove n/a
#
#        # IFSMODSEL low-res, med-res (represents spatial size for reduced cubes for IFS)

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
        '''

        files = []

        return files
