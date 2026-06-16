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
from PIL import Image


class Scales(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):

        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        self.keymap['UTC'] = 'UT'
        self.keymap['PROGNAME'] = 'PROGNAM'
        
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

        # Set spectral resolution list
        self.specres = {
            "LOWRES" : {
                'K':     150,
                'KL':    50, 
                'KLM':   35,
                'L':     80,
                'Ls':    200,
                'M':     200,
                'KLpol': 20
            },
            "MEDRES" : {
                'K':     6000,
                'L':     2500,
                'M':     7000
            }
        }

        # Set pixel scale
        self.pixelscale = {
            "IM": 0.006, # arcsec/pixel
            "IFS": 0.02  # arcsec/spaxel
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
            {'name':'set_wavelengths', 'crit': False},
            {'name':'set_resolution',  'crit': False},
            {'name':'set_pixelscale',  'crit': False},
            {'name':'set_image_stats', 'crit': False},
            #{'name':'set_npixsat',     'crit': False},
            {'name':'set_weather',     'crit': False},
            {'name':'set_oa',          'crit': False},
            {'name':'set_dqa_vers',    'crit': False},
            {'name':'set_dqa_date',    'crit': False},
        ]
        return self.run_functions(funcs)


    def is_ramp_fit(self):
        '''
        Return True/False if image is 2D
        '''

        shape = self.fits_hdu[0].data.shape
        return True if len(shape) == 2 else False


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


    def make_koaid(self):
        '''
        Calls the main class make_koaid() and updates koaid,
        if needed, for raw ramp fit files.
        '''

        koaid = super().make_koaid()
        if koaid:
            if self.is_ramp_fit():
                koaid += '_qramp'

        return koaid


    def create_jpg_from_fits(self, fits_filepath, outdir_path):
        '''
        Basic convert fits primary data to jpg. overrides super class function
        '''

        data = self.fits_hdu[0].data

        if data is None:
            log.info(f"No data in primary HDU of {fits_filepath}")
            return None

        # NAXIS dimensionality from header, fall back to ndim if missing
        naxis = self.get_keyword('NAXIS', default=data.ndim) #hdr.get("NAXIS", data.ndim)

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
            norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=SqrtStretch())
            norm_arr = scaled = norm(result)    # float array in [0, 1]   # norm_arr
    
        # 2b- stretch ~0...1 ensures values are in [0, 1]
        norm_arr = np.clip(norm_arr, 0.0, 1.0)
    
        # builds output filepath
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        jpg_filepath = f'{outdir_path}/{basename}.jpg'

        # option 1: saves faster version, no fig necessary
        # image_eq or norm_arr is final 0-1 float array
#        plt.imsave(jpg_filepath, norm_arr, cmap="gray", format="jpg")
        img8 = (norm_arr * 255).astype(np.uint8)
        im = Image.fromarray(img8)
        im.thumbnail((1024, 1024))
        im.save(jpg_filepath, quality=85)


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
            log.info(f'set_wavelengths: error setting wavelengths from FILTER={filterList}')

        return True


    def set_resolution(self):
        '''
        Using FILTER and MODSLNAM, determine spectral resolution.
        the filter and SI.20260605.62983.38.fits: MODSLNAM = MedRes
        '''

        if self.get_keyword('CAMERA', default='').upper() != "IFS":
            return True

        resolution = 'null'
        count = 0
        mode = self.get_keyword('MODSLNAM', default='').upper()
        if mode in self.specres.keys():
            specres = self.specres[mode]
            filterList = self.get_keyword('FILTER', default='').split('+')

            for fitem in filterList:
                if fitem in specres.keys():
                    resolution = specres[fitem]
                    count += 1

        if count == 1:
            self.set_keyword('SPECRES',resolution,'KOA: Approximate spectral resolution')
        else:
            log.info('set_resolution: error setting SPECRES')

        return True


    def set_pixelscale(self):
        '''
        Set pixel scales
        '''

        dispscal = 'null'
        spatscal = 'null'

        camera = self.get_keyword('CAMERA', default='').upper()
        if camera in self.pixelscale.keys():
            dispscal = self.pixelscale[camera]
            spatscal = dispscal
            self.set_keyword('DISPSCAL',dispscal,'KOA: CCD pixel scale, dispersion')
            self.set_keyword('SPATSCAL',spatscal,'KOA: CCD pixel scale, spatial')
        
        return True


    def has_target_info(self):
        '''
        Does this fits have sensitive target info?
        '''
        return False


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP specific to SCALES.
        '''

        files = []

        return files
