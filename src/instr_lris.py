'''
This is the class to handle all the LRIS specific attributes

https://www2.keck.hawaii.edu/inst/lris/instrument_key_list.html
'''

import instrument
import datetime as dt
import numpy as np
import math
from astropy.convolution import convolve,Box1DKernel
from astropy.io import fits
from astropy import units as u
from astropy.coordinates import SkyCoord
import os
import re

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from PIL import Image
from astropy.visualization import ZScaleInterval, AsinhStretch, SinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize
from mpl_toolkits.axes_grid1 import ImageGrid

import hist_equal2d

import logging
log = logging.getLogger('koadep')


class Lris(instrument.Instrument):

    def __init__(self, instr, filepath, config, db, reprocess, tpx):

        super().__init__(instr, filepath, config, db, reprocess, tpx)

        # Other vars that subclass can overwrite
        self.keyskips   = ['CCDGN00', 'CCDRN00']
        #self.keyskips = ['IM01MN00', 'IM01SD00', 'IM01MD00', 'PT01MN00', 'PT01SD00', 'PT01MD00', 'IM01MN01', 'IM01SD01', 'IM01MD01', 'PT01MN01', 'PT01SD01', 'PT01MD01', 'IM02MN02', 'IM02SD02', 'IM02MD02', 'PT02MN02', 'PT02SD02', 'PT02MD02', 'IM02MN03', 'IM02SD03', 'IM02MD03', 'PT02MN03', 'PT02SD03', 'PT02MD03']


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        ok = True
        if ok: ok = super().run_dqa()
        if ok: ok = self.set_koaimtyp()
        if ok: ok = self.set_ut()
        if ok: ok = self.set_ofname()
        if ok: ok = self.set_frameno()
        if ok: ok = self.set_semester()
        if ok: ok = self.set_prog_info()
        if ok: ok = self.set_propint()
        if ok: ok = self.set_datlevel(0)
        if ok: ok = self.get_nexten()
        if ok: ok = self.set_image_stats_keywords()
        if ok: ok = self.set_weather_keywords()
        if ok: ok = self.set_oa()
        if ok: ok = self.set_npixsat(satVal=65535)
        if ok: ok = self.set_obsmode()
        if ok: ok = self.set_wavelengths()
        if ok: ok = self.set_sig2nois()
        if ok: ok = self.set_ccdtype()
        if ok: ok = self.set_slit_dims()
        if ok: ok = self.set_wcs()
        if ok: ok = self.set_skypa()        
        if ok: ok = self.set_dqa_vers()
        if ok: ok = self.set_dqa_date()
        return ok


    @staticmethod
    def get_dir_list():
        '''
        Function to generate the paths to all the LRIS accounts, including engineering
        Returns the list of paths
        '''
        #todo: note: idl dep searches /s/sdata/2* , though it is known that the dirs are 241/242/243
        #todo: note: There are subdirs /lris11/ thru /lris20/, though it is known that these are not used
        dirs = []
        path = '/s/sdata24'
        for i in range(1,4):
            path2 = path + str(i) + '/lris'
            for i in range(1,10):
                path3 = path2 + str(i)
                dirs.append(path3)
            dirs.append(path2 + 'eng')
        return dirs


    def get_prefix(self):
        '''
        Get FITS file prefix
        '''
        instr = self.get_instr()
        if   instr == 'lrisblue': prefix = 'LB'
        elif instr == 'lris'    : prefix = 'LR'
        else                    : prefix = ''
        return prefix


    def set_instr(self):
        '''
        Override instrument.set_instr since that assumes a single INSTRUME name but we have LRIS and LRISBLUE
        '''

        #direct match (or starts with match)?
        ok = False
        instrume = self.get_keyword('INSTRUME')
        if instrume in ('LRIS', 'LRISBLUE'):
            ok = True
        if (not ok):
            log.error('set_instr: cannot determine if file is from ' + self.instr + '.  UDF!')
        return ok


    def set_ofname(self):
        '''
        Sets OFNAME keyword from OUTFILE and FRAMENO
        '''
        outfile = self.get_keyword('OUTFILE', False)
        frameno = self.get_keyword('FRAMENO', False)
        if outfile == None or frameno == None:
            log.warning('set_ofName: Could not determine OFNAME')
            return False
    
        frameno = str(frameno).zfill(4)
        ofName = f'{outfile}{frameno}.fits'
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')
        return True


    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm
        Calls get_koaimtyp for algorithm
        '''
        koaimtyp = self.get_koaimtyp()
        if (koaimtyp == 'undefined'):
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        return True


    def get_koaimtyp(self):

        #if instrument not defined, return
        imagetyp = 'undefined'
        try:
            instrume = self.get_keyword('INSTRUME')
        except:
            return 'undefined'

        #focus
        slitname = self.get_keyword('SLITNAME')
        outfile = self.get_keyword('OUTFILE')
        if (slitname == 'GOH_LRIS') or (outfile == 'rfoc') or (outfile == 'bfoc'):
            return 'focus'

        #bias
        elaptime = self.get_keyword('ELAPTIME')
        if elaptime == 0:
            return 'bias'

        #flat, dark, wave, object
        try:
            trapdoor = self.get_keyword('TRAPDOOR')
        except:
            return 'undefined'
        graname = self.get_keyword('GRANAME')
        grisname = self.get_keyword('GRISNAME')
        if trapdoor == 'open':
            #is lamp on?
            flimagin = self.get_keyword('FLIMAGIN')
            flspectr = self.get_keyword('FLSPECTR')
            flat1 = self.get_keyword('FLAMP1')
            flat2 = self.get_keyword('FLAMP2')
            #a lamp is on
            if 'on' in [flimagin,flspectr,flat1,flat2]:
                return 'flatlamp'
            else:
                #no lamp on
                if self.get_keyword('AUTOSHUT'):
                    calname = self.get_keyword('CALNAME')
                    if calname in ['ir','hnpb','uv']:
                        return 'polcal'
                    else:
                        return 'object'
                else:
                    return 'undefined'
        elif trapdoor == 'closed':
            #is lamp on?
            lamps = self.get_keyword('LAMPS')
            if lamps not in ['','0',None]:
                if '1' in lamps:
                    if lamps == '0,0,0,0,0,1':
                        return 'flatlamp'
                    else:
                        if instrume == 'LRIS':
                            if graname != 'mirror':
                                return 'arclamp'
                        elif instrume == 'LRISBLUE':
                            if grisname != 'clear':
                                return 'arclamp'
                else:
                    if lamps == '0,0,0,0,0,0':
                        return 'dark'
            else:
                mercury = self.get_keyword('MERCURY')
                neon = self.get_keyword('NEON')
                argon = self.get_keyword('ARGON')
                cadmium = self.get_keyword('CADMIUM')
                zinc = self.get_keyword('ZINC')
                halogen = self.get_keyword('HALOGEN')
                krypton = self.get_keyword('KRYPTON')
                xenon = self.get_keyword('XENON')
                feargon = self.get_keyword('FEARGON')
                deuterium = self.get_keyword('DEUTERI')

                if halogen == 'on':
                    return 'flatlamp'
                elif 'on' in [neon,argon,cadmium,zinc,krypton,xenon,feargon,deuterium]:
                    if instrume == 'LRIS':
                        if graname != 'mirror':
                            return 'arclamp'
                    elif instrume == 'LRISBLUE':
                        if grisname != 'clear':
                            return 'arclamp'
                elif all(element == 'off' for element in [neon,argon,cadmium,zinc,halogen,krypton,xenon,feargon,deuterium]):
                    return 'dark'

        #undefined
        return 'undefined'


    def set_obsmode(self):
        '''
        Determine observation mode
        '''
        grism = self.get_keyword('GRISNAME')
        grating = self.get_keyword('GRANAME')
        angle = self.get_keyword('GRANGLE')
        instr = self.get_keyword('INSTRUME')

        if instr == 'LRISBLUE':
            if ('cl' in grism) or ('NB' in grism):
                obsmode = 'IMAGING'
            else:
                obsmode = 'SPEC'
        elif instr == 'LRIS':
            if (grating == 'mirror'):
                obsmode = 'IMAGING'
            else:
                obsmode = 'SPEC'

        self.set_keyword('OBSMODE',obsmode,'KOA: Observation Mode (Imaging or Spec)')
        return True


    def set_wavelengths(self):
        '''
        Get blue, center, and red wavelengths [WAVEBLUE,WAVECNTR,WAVERED]
        '''
        is_null = False

        instr = self.get_keyword('INSTRUME')
        slitname = self.get_keyword('SLITNAME')
        obsmode = self.get_keyword('OBSMODE')
        grating = self.get_keyword('GRANAME')
        grism = self.get_keyword('GRISNAME')
        slitmask = str(self.get_keyword('SLITMASK', default=''))

        #Imaging mode
        if obsmode == 'IMAGING':
            if instr == 'LRIS':
                flt = self.get_keyword('REDFILT')
                wavearr = dict({'clear':[3500,9000],
                                'B':[3800,5300],
                                'V':[4800,6600],
                                'R':[5500,8200],
                                'Rs':[6000,7500],
                                'I':[6800,8400],
                                'GG495':[4800,4950],
                                'OG570':[5500,5700],
                                'RG850':[8200,8500],
                                'NB4000':[3800,4200],
                                'NB5390':[5350,5400],
                                'NB6741':[6700,6800],
                                'NB8185':[8150,8250],
                                'NB8560':[8500,8650],
                                'NB9135':[9100,9200],
                                'NB9148':[9050,9250],
                                'NB4325':[9050,9520]})
            elif instr == 'LRISBLUE':
                flt = self.get_keyword('BLUFILT')
                wavearr = dict({'clear':[3000,6500],
                                'U':[3050,4000],
                                'B':[3900,4900],
                                'V':[5800,6600],
                                'G':[4100,5300],
                                'SP580':[0,0],
                                'NB4170':[0,0]})
            if flt == 'Clear':
                flt = 'clear'

        #Spectroscopy mode
        else:
            if instr == 'LRIS':
                wlen = self.get_keyword('WAVELEN')
                if not wlen: return True
                wavearr = dict({'150/7500':[wlen-12288/2,wlen+12288/2],
                                '300/5000':[wlen-6525/2,wlen+6525/2],
                                '400/8500':[wlen-4762/2,wlen+4762/2],
                                '600/5000':[wlen-3275/2,wlen+3275/2],
                                '600/7500':[wlen-3275/2,wlen+3275/2],
                                '600/10000':[wlen-3275/2,wlen+3275/2],
                                '831/8200':[wlen-2375/2,wlen+2375/2],
                                '900/5500':[wlen-2175/2,wlen+2175/2],
                                '1200/7500':[wlen-1638/2,wlen+1638/2],
                                '1200/9000':[wlen-1638/2,wlen+1638/2]})
                dateobs = self.get_keyword('DATE-OBS')
                date = dt.datetime.strptime(dateobs,'%Y-%M-%d')
                newthreshold = dt.datetime(2015,5,14)
                #if observing date before May 14th, 2015, use different set of gratings
                if date < newthreshold:
                    wavearr = dict({'150/7500':[wlen-9830/2,wlen+9830/2],
                                    '158/8500':[wlen-9830/2,wlen+9830/2],
                                    '300/5000':[wlen-5220/2,wlen+5220/2],
                                    '400/8500':[wlen-3810/2,wlen+3810/2],
                                    '600/5000':[wlen-2620/2,wlen+2620/2],
                                    '600/7500':[wlen-2620/2,wlen+2620/2],
                                    '600/10000':[wlen-2620/2,wlen+2620/2],
                                    '831/8200':[wlen-1900/2,wlen+1900/2],
                                    '900/5500':[wlen-1740/2,wlen+1740/2],
                                    '1200/7500':[wlen-1310/2,wlen+1310/2]})
            elif instr == 'LRISBLUE':
                #longslit
                if 'long_' in slitmask or 'pol_' in slitmask:
                    wavearr = dict({'300/5000':[1570,7420],
                                    '400/3400':[1270,5740],
                                    '600/4000':[3010,5600],
                                    '1200/3400':[2910,3890]})
                else:
                    wavearr = dict({'300/5000':[2210,8060],
                                    '400/3400':[1760,6220],
                                    '600/4000':[3300,5880],
                                    '1200/3400':[3010,4000]})
            else:
                return True

        #dichroic cutoff
        #NOTE: Elysia fixed bug in IDL code was incorrectly not truncating the wavelength range 
        #bc the dichroic wavelength is in nanometers and the wavelength range is in angstroms.
        dichname = self.get_keyword('DICHNAME')
        if   dichname == '460': minmax = 4874
        elif dichname == '500': minmax = 5091
        elif dichname == '560': minmax = 5696
        elif dichname == '680': minmax = 6800
        else                  : minmax = 0

        #determine wavelength range
        if obsmode == 'IMAGING':
            if flt in wavearr: waveblue, wavered = wavearr.get(flt)
            else             : return True
        elif obsmode == 'SPEC':
            if   grating in wavearr: waveblue, wavered = wavearr.get(grating)
            elif grism in wavearr  : waveblue, wavered = wavearr.get(grism)
            else                   : return True

        #if wavelength range encompasses dichroic cutoff
        #LRIS: minmax to wavered
        #LRISBLUE: waveblue to minmax
        if instr == 'LRIS':
            if waveblue < minmax:
                waveblue = minmax
        elif instr == 'LRISBLUE':
            if wavered > minmax:
                wavered = minmax

        #round to the nearest 10 angstroms
        wavered  = int(round(np.round(wavered,-1)))
        waveblue = int(round(np.round(waveblue,-1)))
        wavecntr = int(round((waveblue + wavered)/2))

        self.set_keyword('WAVERED', wavered, 'KOA: Red wavelength')
        self.set_keyword('WAVEBLUE',waveblue,'KOA: Blue wavelength')
        self.set_keyword('WAVECNTR',wavecntr,'KOA: Center wavelength')

        return True

    def set_ccdtype(self):
        '''
        Set CCD gain and read noise
        '''
        # NOTE: It looks like the IDL version for LRIS BLUE was incorrectly writing "00" index 
        # versions of these keywords to the header that didn't match the metadata file which only
        #has 1-4.  And it was writing null for the "04" version.  We are mimicing this behavior below.
        ccdgain = 'null'
        readnoise = 'null'

        #gain and read noise values per extension
        gainblue = [1.55,  1.56,  1.63,  1.70]
        rnblue   = [3.9,   4.2,   3.6,   3.6]
        gainred  = [1.255, 1.180, 1.191, 1.162]
        rnred    = [4.64,  4.76,  4.54,  4.62]

        #red or blue?
        instr = self.get_keyword('INSTRUME')
        if instr == 'LRISBLUE':
            gain = gainblue
            rn = rnblue
        elif instr == 'LRIS':
            gain = gainred
            rn = rnred

        for ext in range(1, self.nexten+1):
            amploc = int(self.get_keyword('AMPLOC',ext=ext))
            self.set_keyword(f'CCDGN0{amploc}', gain[amploc-1], 'KOA: CCD Gain')
            self.set_keyword(f'CCDRN0{amploc}', rn[amploc-1], 'KOA: CCD Read Noise')
        return True

    def set_sig2nois(self):
        '''
        Calculates S/N for middle CCD image
        '''

        #NOTE: Decided to remove this calc from KOA so setting to null.
        self.set_keyword('SIG2NOIS', 'null', 'KOA: S/N estimate near image spectral center')
        return True

        # if self.nexten == 0: return True

        # #find middle extension
        # ext = int(np.floor(self.nexten/2.0))
        # image = self.fits_hdu[ext].data

        # naxis1 = self.get_keyword('NAXIS1',ext=ext)
        # naxis2 = self.get_keyword('NAXIS2',ext=ext)
        # postpix = self.get_keyword('POSTPIX', default=0)
        # precol = self.get_keyword('PRECOL', default=0)

        # numamps = self.get_numamps()
        # nx = (naxis2 - numamps*(precol + postpix))
        # c = [naxis1/2, 1.17*nx/2]
        # wsize = 10
        # spaflux = []

        # #necessary?
        # if c[1] > naxis1-wsize:
        #     c[1] = c[0]

        # for i in range(wsize, int(naxis1)-wsize):
        #     spaflux.append(np.median(image[int(c[1])-wsize:int(c[1])+wsize, i]))

        # spaflux = convolve(spaflux,Box1DKernel(3))
        # maxflux = np.max(spaflux[precol:naxis1-1])
        # minflux = np.min(spaflux[precol:naxis1-1])

        # sig2nois = np.fix(np.sqrt(np.abs(maxflux - minflux)))

        # self.set_keyword('SIG2NOIS', sig2nois, 'KOA: S/N estimate near image spectral center')

        # return True

    def get_numamps(self):
        '''
        Determine number of amplifiers
        '''
        #separate logic for LRISBLUE
        if self.get_keyword('INSTRUME') == 'LRISBLUE':
            amplist = self.get_keyword('AMPLIST', default='').strip()
            if   amplist == '1,0,0,0':  numamps = 1
            elif amplist == '2,0,0,0':  numamps = 1
            elif amplist == '2,1,0,0':  numamps = 2
            elif amplist == '1,3,0,0':  numamps = 1
            elif amplist == '2,4,0,0':  numamps = 1
            elif amplist == '1,4,0,0':  numamps = 2
            else                     :  numamps = 0
            return numamps

        #lris red
        ampmode = self.get_keyword('AMPMODE', default='')
        if   'SINGLE:L' in ampmode: numamps = 1
        elif 'SINGLE:R' in ampmode: numamps = 1
        elif 'DUAL:L+R' in ampmode: numamps = 2
        else                      : numamps = 0
        return numamps

    def get_nexten(self):
        '''
        Determine number of FITS HDU extensions
        '''
        self.nexten = len(self.fits_hdu)-1
        return True

    def set_wcs(self):

        #only do this for IMAGING
        obsmode = self.get_keyword('OBSMODE')
        if obsmode != 'IMAGING': 
            return True        

        pixelscale = 0.135 #arcsec
        rotposn = self.get_keyword('ROTPOSN')
        poname  = self.get_keyword('PONAME')
        ra      = self.get_keyword('RA')
        dec     = self.get_keyword('DEC')
        pixcorrect = lambda x: (x/pixelscale) + 1024

        #dictionary of xim and yim only
        podict = dict({'REF':[380.865,71.44],
                       'REFO':[-377.22,72.52],
                       'LRIS':[3.97,-309.82],
                       'slitb':[-14.41,-263.74],
                       'slitc':[31.85,-262.58],
                       'POL':[3.91,-273.12],
                       'LRISB':[-53.11,-310.54],
                       'PICKOFF':[27.95,-260.63],
                       'MIRA':[3.67,-300.34],
                       'BEDGE':[20.25,-260.68],
                       'TEDGE':[-0.65,-263.68],
                       'UNDEFINED':[-53.11,-320.54]})
        if poname not in podict.keys():
            poname = 'UNDEFINED'
        xim,yim = podict.get(poname)
        if poname == 'REF':
            xcen = 485
            ycen = 520
        elif poname == 'REFO':
            xcen = 512
            ycen = 512
        else:
            xcen = pixcorrect(yim+308.1)
            ycen = pixcorrect(3.4-xim)

        #for each FITS header extension, calculate CRPIX1/2 and CDELT1/2
        for i in range(1,self.nexten+1):
            crpix1 = self.get_keyword('CRPIX1',ext=i)
            crpix2 = self.get_keyword('CRPIX2',ext=i)
            crval1 = self.get_keyword('CRVAL1',ext=i)
            crval2 = self.get_keyword('CRVAL2',ext=i)
            cd11   = self.get_keyword('CD1_1',ext=i)
            cd22   = self.get_keyword('CD2_2',ext=i)
            naxis1 = self.get_keyword('NAXIS1',ext=i)
            naxis2 = self.get_keyword('NAXIS2',ext=i)            

            crpix1_new = crpix1 + ((xcen - crval1)/cd11)
            crpix2_new = crpix2 + ((ycen - crval2)/cd22)
            cdelt1_new = cd11 * pixelscale
            cdelt2_new = cd22 * pixelscale
            self.set_keyword('CRPIX1',crpix1_new,'KOA: CRPIX1',ext=i)
            self.set_keyword('CRPIX2',crpix2_new,'KOA: CRPIX2',ext=i)
            self.set_keyword('CDELT1',cdelt1_new,'KOA: CDELT1',ext=i)
            self.set_keyword('CDELT2',cdelt2_new,'KOA: CDELT2',ext=i)
            self.set_keyword('CTYPE1','RA---TAN','KOA: CTYPE1',ext=i)
            self.set_keyword('CTYPE2','DEC--TAN','KOA: CTYPE2',ext=i)
            self.set_keyword('CROTA2',rotposn,'KOA: Rotator position',ext=i)

            #set crval1/2 after we have used their original values
            coord = SkyCoord(f'{ra} {dec}', unit=(u.hourangle, u.deg))
            ra_deg  = coord.ra.degree
            dec_deg = coord.dec.degree
            self.set_keyword('CRVAL1',ra_deg,'KOA: CRVAL1',ext=i)
            self.set_keyword('CRVAL2',dec_deg,'KOA: CRVAL2',ext=i)

        return True

    def set_skypa(self):
        '''
        Calculate the HIRES slit sky position angle
        '''

        # Detemine rotator, parallactic and elevation angles
        offset = 270.0
        irot2ang = self.get_keyword('IROT2ANG', False)
        parang = self.get_keyword('PARANG', False)
        el = self.get_keyword('EL', False)

        # Skip if one or more values not found
        if irot2ang == None or parang == None or el == None:
            log.info('set_skypa: Could not set skypa')
            return True
        skypa = (2.0 * float(irot2ang) + float(parang) + float(el) + offset) % (360.0)
        log.info('set_skypa: Setting skypa')
        self.set_keyword('SKYPA', round(skypa, 4), 'KOA: Position angle on sky (deg)')

        return True

    def set_slit_dims(self):
        '''
        Set SLITLEN, SLITWIDT, SPECRES, SPATSCAL
        '''
        slitname = self.get_keyword('SLITNAME')
        if slitname in ['GOH_LRIS','direct']:
            return True
        spatscal = 0.135
        wavelen = self.get_keyword('WAVECNTR')
        slitdict = dict({'long_0.7':[175,0.7],
                         'long_1.0':[175,1.0],
                         'long_1.5':[175,1.5],
                         'long_8.7':[175,8.7],
                         'pol_1.0':[25,1.0],
                         'pol_1.5':[25,1.5]})
        try:
            [slitlen,slitwidt] = slitdict.get(slitname)
        except:
            slitlen,slitwidt = 'null','null'

        dispersion = 0
        fwhm = 0
        instr = self.get_keyword('INSTRUME')
        if instr ==  'LRIS':
            grating = self.get_keyword('GRANAME')
            gratdict = dict({'150/7500':[3.00,0],
                             '300/5000':[0,9.18],
                             '400/8500':[1.16,6.90],
                             '600/5000':[0.80,4.70],
                             '600/7500':[0.80,4.70],
                             '600/10000':[0.80,4.70],
                             '831/8200':[0.58,0],
                             '900/5500':[0.53,0],
                             '1200/7500':[0.40,0],
                             '1200/9000':[0.40,0]})
            try:
                [dispersion,fwhm] = gratdict.get(grating)
            except:
                dispersion,fwhm = 0,0
        elif instr == 'LRISBLUE':
            grism = self.get_keyword('GRISNAME')
            grismdict = dict({'300/5000':[1.43,8.80],
                              '400/3400':[1.09,6.80],
                              '600/4000':[0.63,3.95],
                              '1200/3400':[0.24,1.56]})
            try:
                [dispersion,fwhm] = grismdict.get(grism)
            except:
                dispersion,fwhm = 0,0
        specres = 'null'
        slit = 1.0
        if slitwidt != 'null':
            slit = slitwidt
        slitpix = slit/spatscal
        deltalam = dispersion * slitpix
        if dispersion != 0:
            specres = round(wavelen/deltalam,-1)
        if fwhm != 0:
            specres = round((wavelen/fwhm)/slit,-2)

        self.set_keyword('SLITLEN',slitlen,'KOA: Slit length')
        self.set_keyword('SLITWIDT',slitwidt,'KOA: Slit width')
        self.set_keyword('SPECRES',specres,'KOA: Spectral resolution')
        self.set_keyword('SPATSCAL',spatscal,'KOA: Spatial resolution')
        self.set_keyword('DISPSCAL',dispersion,'KOA: Dispersion scale')

        return True

    def set_npixsat(self, satVal=None):
        '''
        Determines number of saturated pixels and adds NPIXSAT to header
        '''
        if satVal == None:
            satVal = self.get_keyword('SATURATE')

        if satVal == None:
            log.warning("set_npixsat: Could not find SATURATE keyword")
        else:
            nPixSat = 0
            for ext in range(1, self.nexten+1):
                image = self.fits_hdu[ext].data
                pixSat = image[np.where(image >= satVal)]
                nPixSat += len(image[np.where(image >= satVal)])

            self.set_keyword('NPIXSAT', nPixSat, 'KOA: Number of saturated pixels')

        return True


    def set_image_stats_keywords(self):
        '''
        Get mean, median, and standard deviation of middle 225 (15x15) pixels of image and postscan
        NOTE: We use integer division throughout to mimic IDL code.
        ''' 

        # TRANSPOSED IMAGE

        #precol        imx         postpix
        #.................................  ^
        #.      .                 .      .  |
        #.      .                 .      .  |
        #.      .                 .      .  |
        #.      .                 .      . NAXIS2
        #.      .                 .      .  |
        #.      .                 .      .  |
        #.      .                 .      .  |
        #.................................  v
        #<------------NAXIS1------------->

        #cycle through FITS extensions
        for ext in range(1,self.nexten+1):

            #get image header and image
            header = self.fits_hdu[ext].header
            image = np.array(self.fits_hdu[ext].data)

            #find widths of pre/postscan regions, whole image dimensions
            precol = self.get_keyword('PRECOL')
            postpix = self.get_keyword('POSTPIX')
            binning = self.get_keyword('BINNING')
            naxis1 = self.get_keyword('NAXIS1',ext=ext)
            naxis2 = self.get_keyword('NAXIS2',ext=ext)

            # Size of sampling box: nx = 15 & ny = 15
            xbin = 1
            ybin = 1
            if binning and ',' in binning:
                binning = binning.replace(' ', '').split(',')
                xbin = int(binning[0])
                ybin = int(binning[1])
            nx = postpix//xbin
            nx = nx - nx//3
            if nx > 15 or nx == 0: nx = 15
            ny = nx

            # x: number of imaging pixels and start of postscan 
            nxi = naxis1 - postpix//xbin - precol//xbin
            px1 = precol//xbin + nxi - 1

            # center of imaging pixels and postscan 
            cxi = precol//xbin + nxi//2
            cyi = naxis2//2 
            cxp = px1 + postpix//xbin//2
       
            #transpose image to correspond with precol/postpix parameters
            image = np.transpose(image)

            #take statistics of middle  pixels of image
            x1 = int(cxi-nx//2)
            x2 = int(cxi+nx//2)
            y1 = int(cyi-ny//2)
            y2 = int(cyi+ny//2)
            imsample = image[x1:x2+1, y1:y2+1]
            im1mn    = np.mean(imsample)
            im1stdv  = np.std(imsample)
            im1md    = np.median(imsample)

            #take statistics of middle pixels of postscan
            x1 = int(cxp-nx//2)
            x2 = int(cxp+nx//2)
            y1 = int(cyi-ny//2)
            y2 = int(cyi+ny//2)
            pssample = image[x1:x2+1, y1:y2+1]
            pst1mn   = np.mean(pssample)
            pst1stdv = np.std(pssample)
            pst1md   = np.median(pssample)

            #get ccdloc and adjust for type
            ccdloc = int(self.get_keyword('CCDLOC',ext=ext))
            if self.get_keyword('INSTRUME') == 'LRISBLUE':
                ccdloc += 1

            #get amplifier location and adjust for type
            #NOTE: In order to mimic incorrect IDL behavior, we are not subtracting 1 from AMPLOC.
            #This means red images will have null values for IM01MN02 and IM02MN04 in metadata but header will have these values.
            amploc = int(self.get_keyword('AMPLOC',ext=ext))
            #if self.get_keyword('INSTRUME') == 'LRIS': amploc -= 1

            #create and set image keywords
            #todo: confirm our fix when there are only 2 extentions is correct (idl code looks to have shifted red values up one)
            mnkey   = f'IM0{ccdloc}MN0{amploc}'
            stdvkey = f'IM0{ccdloc}SD0{amploc}'
            mdkey   = f'IM0{ccdloc}MD0{amploc}'
            self.set_keyword(mnkey,   str(round(im1mn, 2)),   f'KOA: Imaging mean CCD {ccdloc}, amp location {amploc}')
            self.set_keyword(stdvkey, str(round(im1stdv, 2)), f'KOA: Imaging standard deviation CCD {ccdloc}, amp location {amploc}')
            self.set_keyword(mdkey,   str(round(im1md, 2)),   f'KOA: Imaging median CCD {ccdloc}, amp location {amploc}')

            #create and set postscan keywords
            mnkey   = f'PT0{ccdloc}MN0{amploc}'
            stdvkey = f'PT0{ccdloc}SD0{amploc}'
            mdkey   = f'PT0{ccdloc}MD0{amploc}'
            self.set_keyword(mnkey,   str(round(pst1mn, 2)),   f'KOA: Postscan mean CCD {ccdloc}, amp location {amploc}')
            self.set_keyword(stdvkey, str(round(pst1stdv, 2)), f'KOA: Postscan standard deviation CCD {ccdloc}, amp location {amploc}')
            self.set_keyword(mdkey,   str(round(pst1md, 2)),   f'KOA: Postscan median CCD {ccdloc}, amp location {amploc}')

        return True


    def create_jpg_from_fits(self, fits_filepath, outdir):
        '''
        Overriding instrument default function
        Tile images horizontally in order from left to right. 
        Use DETSEC keyword to figure out data order/position
        '''

        #open
        hdus = fits.open(fits_filepath, ignore_missing_end=True)

        #needed hdr vals
        hdr0 = hdus[0].header
        binning  = hdr0['BINNING'].split(',')
        precol   = int(hdr0['PRECOL'])   // int(binning[0])
        postpix  = int(hdr0['POSTPIX'])  // int(binning[0])
        preline  = int(hdr0['PRELINE'])  // int(binning[1])
        postline = int(hdr0['POSTLINE']) // int(binning[1])

        #get extension order (uses DETSEC keyword)
        ext_order = Lris.get_ext_data_order(hdus)
        assert ext_order, "ERROR: Could not determine extended data order"

        #loop thru extended headers in order, create png and add to list in order
        interval = ZScaleInterval()
        vmin = None
        vmax = None
        alldata = None
        for i, ext in enumerate(ext_order):

            data = hdus[ext].data
            hdr  = hdus[ext].header

            #calc bias array from postpix area
            sh = data.shape
            x1 = 0
            x2 = sh[0]
            y1 = sh[1] - postpix + 1
            y2 = sh[1] - 1
            bias = np.median(data[x1:x2, y1:y2], axis=1)
            bias = np.array(bias, dtype=np.int64)

            #subtract bias
            data = data - bias[:,None]

            #get min max of each ext (not including pre/post pixels)
            #NOTE: using sample box that is 90% of full area
            #todo: should we take an average min/max of each ext for balancing?
            sh = data.shape
            x1 = int(preline          + (sh[0] * 0.10))
            x2 = int(sh[0] - postline - (sh[0] * 0.10))
            y1 = int(precol           + (sh[1] * 0.10))
            y2 = int(sh[1] - postpix  - (sh[1] * 0.10))
            tmp_vmin, tmp_vmax = interval.get_limits(data[x1:x2, y1:y2])
            if vmin == None or tmp_vmin < vmin: vmin = tmp_vmin
            if vmax == None or tmp_vmax > vmax: vmax = tmp_vmax
            if vmin < 0: vmin = 0

            #remove pre/post pix columns
            data = data[:,precol:data.shape[1]-postpix]

            #flip data left/right 
            #NOTE: This should come after removing pre/post pixels
            ds = Lris.get_detsec_data(hdr['DETSEC'])
            if ds and ds[0] > ds[1]: 
                data = np.fliplr(data)
            if ds and ds[2] > ds[3]: 
                data = np.flipud(data)

            #concatenate horizontally
            if i==0: alldata = data
            else   : alldata = np.append(alldata, data, axis=1)

        #filepath vars
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        out_filepath = f'{outdir}/{basename}.jpg'

        #bring in min/max by 2% to help ignore large areas of black or overexposed spots
        #todo: this does not achieve what we want
        # minmax_adjust = 0.02
        # vmin += int((vmax - vmin) * minmax_adjust)
        # vmax -= int((vmax - vmin) * minmax_adjust)

        #normalize, stretch and create jpg
        norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
        dpi = 100
        width_inches  = alldata.shape[1] / dpi
        height_inches = alldata.shape[0] / dpi
        fig = plt.figure(figsize=(width_inches, height_inches), frameon=False, dpi=dpi)
        ax = fig.add_axes([0, 0, 1, 1]) #this forces no border padding; bbox_inches='tight' doesn't really work
        plt.axis('off')
        plt.imshow(alldata, cmap='gray', origin='lower', norm=norm)
        plt.savefig(out_filepath, quality=92)
        plt.close()


    def create_jpg_from_fits_HIST(self, fits_filepath, outdir):
        '''
        Overriding instrument default function
        Tile images horizontally in order from left to right. 
        Use DETSEC keyword to figure out data order/position
        '''
        #NOTE: Not using this right now until we decide if it is better than default create_jpg_from_fits
        
        #open
        hdus = fits.open(fits_filepath, ignore_missing_end=True)

        #needed hdr vals
        hdr0 = hdus[0].header
        binning  = hdr0['BINNING'].split(',')
        precol   = int(hdr0['PRECOL'])   // int(binning[0])
        postpix  = int(hdr0['POSTPIX'])  // int(binning[0])
        preline  = int(hdr0['PRELINE'])  // int(binning[1])
        postline = int(hdr0['POSTLINE']) // int(binning[1])

        #get extension order (uses DETSEC keyword)
        ext_order = Lris.get_ext_data_order(hdus)
        assert ext_order, "ERROR: Could not determine extended data order"

        #loop thru extended headers in order, create png and add to list in order
        vmin = None
        vmax = None
        alldata = None
        for i, ext in enumerate(ext_order):

            data = hdus[ext].data
            hdr  = hdus[ext].header

            #remove pre/post pix columns
            data = data[:,precol:data.shape[1]-postpix]

            #flip data left/right 
            #NOTE: This should come after removing pre/post pixels
            ds = Lris.get_detsec_data(hdr['DETSEC'])
            if ds and ds[0] > ds[1]: 
                data = np.fliplr(data)
            if ds and ds[2] > ds[3]: 
                data = np.flipud(data)

            #concatenate horizontally
            if i==0: alldata = data
            else   : alldata = np.append(alldata, data, axis=1)

        #hist
        heq2d = hist_equal2d.HistEqual2d()
        alldata = heq2d._perform(alldata)

        #filepath vars
        basename = os.path.basename(fits_filepath).replace('.fits', '')
        out_filepath = f'{outdir}/{basename}.jpg'

        #normalize, stretch and create jpg
        dpi = 100
        width_inches  = alldata.shape[1] / dpi
        height_inches = alldata.shape[0] / dpi
        fig = plt.figure(figsize=(width_inches, height_inches), frameon=False, dpi=dpi)
        ax = fig.add_axes([0, 0, 1, 1]) #this forces no border padding; bbox_inches='tight' doesn't really work
        plt.axis('off')
        plt.imshow(alldata, cmap='gray', origin='lower')
        plt.savefig(out_filepath, quality=92)
        plt.close()


    @staticmethod
    def get_ext_data_order(hdus):
        '''
        Use DETSEC keyword to figure out true order of extension data for horizontal tiling
        '''
        key_orders = {}
        for i in range(1, len(hdus)):
            ds = Lris.get_detsec_data(hdus[i].header['DETSEC'])
            if not ds: return None
            key_orders[ds[0]] = i

        orders = []
        for key in sorted(key_orders):
            orders.append(key_orders[key])
        return orders


    @staticmethod
    def get_detsec_data(detsec):
        '''
        Parse DETSEC string for x1, x2, y1, y2
        '''
        match = re.search( r'(-?\d+):(-?\d+),(-?\d+):(-?\d+)', detsec)
        if not match:
            return None
        else:
            x1 = int(match.groups(1)[0])
            x2 = int(match.groups(1)[1])
            y1 = int(match.groups(1)[2])
            y2 = int(match.groups(1)[3])
            return [x1, x2, y1, y2]

