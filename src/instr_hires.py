'''
This is the class to handle all the HIRES specific attributes
'''

import instrument
import datetime as dt
from common import *
from math import ceil, floor
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from PIL import Image
from astropy.visualization import ZScaleInterval, AsinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize
import scipy

import logging
main_logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class Hires(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):

        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

        # Set any unique keyword index values here
        self.keymap['OFNAME']   = 'OUTFILE'


    def run_dqa(self):
        '''Run all DQA checks unique to this instrument.'''

        funcs = [
            {'name':'set_telnr',        'crit': True},
            {'name':'set_ut',           'crit': True}, # may need to delete duplicate UTC?
            {'name':'set_utend',        'crit': True},
            {'name':'set_ofName',       'crit': True},
            {'name':'set_semester',     'crit': True},
            {'name':'set_prog_info',    'crit': True},
            {'name':'set_propint',      'crit': True},
            {'name':'fix_propint',      'crit': True},           
            #{'name':'set_numamps',      'crit': False},
            #{'name':'set_numccds',      'crit': False}, # needed?
            {'name':'set_koaimtyp',      'crit': False}, # imagetyp
            #{'name':'set_blank',        'crit': False},
            {'name':'fix_binning',      'crit': False},
            {'name':'set_wavelengths',  'crit': False},
            {'name':'set_instr_status', 'crit': False}, # inststat
            {'name':'set_weather',      'crit': False},
            {'name':'set_image_stats',  'crit': False}, # IM* and PST*, imagestat
            {'name':'set_npixsat',      'crit': False,  'args': {'satVal':65535.0}},
            {'name':'set_sig2nois',     'crit': False},
            {'name':'set_slit_values',  'crit': False},
            {'name':'set_gain_and_rn',  'crit': False}, # ccdtype
            {'name':'set_skypa',        'crit': False}, # skypa
            {'name':'set_subexp',       'crit': False},
            {'name':'set_roqual',       'crit': False},
            {'name':'set_oa',           'crit': False},
            {'name':'set_dqa_date',     'crit': False},
            {'name':'set_dqa_vers',     'crit': False},
            {'name':'set_datlevel',     'crit': False,  'args': {'level':0}},
        ]
        return self.run_functions(funcs)


    def get_dir_list(self):
        '''
        Function to generate the paths to all the HIRES accounts, including engineering
        Returns the list of paths
        '''
        dirs = []
        path = '/s/sdata12'
        for i in range(5,8):
            joinSeq = (path, str(i), '/hires')
            path2 = ''.join(joinSeq)
            for j in range(1,10):
                joinSeq = (path2, str(j))
                path3 = ''.join(joinSeq)
                dirs.append(path3)
            joinSeq = (path2, 'eng')
            path3 = ''.join(joinSeq)
            dirs.append(path3)
        return dirs

    def get_prefix(self):
        '''
        Set prefix to HI if this is a HIRES file
        '''

        instr = self.get_keyword('INSTRUME')
        if 'hires' in instr.lower():
            prefix = 'HI'
        else:
            prefix = ''
        return prefix


    def set_koaimtyp(self):
        '''
        Add KOAIMTYP based on algorithm
        Calls get_koaimtyp for algorithm
        '''

        koaimtyp = self.get_koaimtyp()
        if (koaimtyp == 'undefined'):
            main_logger.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")

        #update keyword
        self.set_keyword('IMAGETYP', koaimtyp, 'KOA: Image type')
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')
        
        return True

        
    def get_koaimtyp(self):
        '''
        Sets koaimtyp based on keyword values
        '''
        
        koaimtyp = 'undefined'
        if self.get_keyword('AUTOSHUT', False) == None: return koaimtyp
        if self.get_keyword('LAMPNAME', False) == None: return koaimtyp
        if self.get_keyword('LMIRRIN', False) == None:  return koaimtyp
        if self.get_keyword('DARKCLOS', False) == None: return koaimtyp
        if self.get_keyword('TTIME', False) == None:    return koaimtyp

        lampname = self.get_keyword('LAMPNAME', False)
        if lampname == 'undefined':
            qtz1 = self.get_keyword('LAMPQTZ1', False)
            qtz2 = self.get_keyword('LAMPQTZ2', False)
            if qtz1 == True or qtz2 == True:
                lampname = 'quartz'
        ttime = self.get_keyword('TTIME', False)
        lmirrin = self.get_keyword('LMIRRIN', False)
        darkclos = self.get_keyword('DARKCLOS', False)
        
        if self.get_keyword('AUTOSHUT', False) == 0:
            lampOn = ''
            if lampname != 'none' and (lmirrin != 0 or darkclos != 1):
                lampOn = '_lamp_on'
            if ttime == 0: koaimtyp = ''.join(('bias', lampOn))
            else:          koaimtyp = ''.join(('dark', lampOn))
            return koaimtyp

        deckname = self.get_keyword('DECKNAME', False)
        catcur1 = self.get_keyword('CATCUR1', False)
        catcur2 = self.get_keyword('CATCUR2', False)
        hatclos = self.get_keyword('HATCLOS', False)

        if deckname == None or catcur1 == None or catcur2 == None or hatclos == None:
            return koaimtyp

        xcovclos = self.get_keyword('XCOVCLOS', False)
        ecovclos = self.get_keyword('ECOVCLOS', False)

        if 'quartz' in lampname:
            if deckname == 'D5':              koaimtyp = 'trace'
            else:                             koaimtyp = 'flatlamp'
            if lmirrin == 0 and hatclos == 0: koaimtyp = 'object_lamp_on'
            if lmirrin == 0 and hatclos == 1: koaimtyp = 'undefined'
            return koaimtyp
        elif 'ThAr' in lampname:
            catcur = catcur1
            if lampname == 'ThAr2': catcur = catcur2
            if catcur >= 5.0:
                koaimtyp = 'arclamp'
                if deckname == 'D5': koaimtyp = 'focus'
            else: koaimtyp = 'undefined'
            if lmirrin == 0 and hatclos == 0:   koaimtyp = 'object_lamp_on'
            if lmirrin == 0 and hatclos == 1:   koaimtyp = 'undefined'
            if xcovclos == 1 and ecovclos == 1: koaimtyp = 'undefined'
            return koaimtyp
        elif 'undefined' in lampname:
            return 'undefined'

        if hatclos == 1:
            koaimtyp = 'dark'
            if ttime == 0: koaimtyp = 'bias'
            return koaimtyp
        
        if ttime == 0: return 'bias'
        
        return 'object'


#    def set_blank(self):
#        '''
#        If BLANK keyword does not exist, create and set to -32768
#        '''
#        
#        if self.get_keyword('Blank', False) != None: return True
#
#        main_logger.info('set_blank: Creating BLANK keyword with value -32768')
#
#        #add keyword
#        self.set_keyword('BLANK', -32768, 'KOA: ')
#        
#        return True


    def fix_binning(self):
        '''
        Remove spaces from BINNING value and update self.get_keyword
        '''

        binning = self.get_keyword('BINNING', False)

        if ' ' not in binning: return True

        comment = ' '.join*(('KOA: Keyword value changed from', binning))
        binning = binning.replace(' ', '')
        self.set_keyword('BINNING', binning, comment)

        return True


    def set_ofName(self):
        '''
        Sets OFNAME keyword from OUTFILE and FRAMENO
        '''
        outfile = self.get_keyword('OUTFILE', False)
        frameno = self.get_keyword('FRAMENO', False)
        if outfile == None or frameno == None:
            self.log_error('SET_OFNAME_ERROR')
            ofname = ''
            return False
        
        frameno = str(frameno).zfill(4)
        ofName = ''.join((outfile, frameno, '.fits'))
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')

        return True


    def set_wavelengths(self):
        '''
        Determine and set wavelength range of spectum
        '''

        #set pixel and CCD size
        psize = 0.015
        npix = 3072.0

        #make sure stages are homed
        if not self.get_keyword('XDISPERS'):
            xdispers = 'RED'
        else:
            xdispers = self.get_keyword('XDISPERS').strip()

        keyflag=1
        keyvars = ['','','','','']
        for key_i,key_val in enumerate(['XDCAL','ECHCAL','XDSIGMAI','XDANGL','ECHANGL']):
            if ((key_val != 'ECHANGL' and key_val != 'XDANGL') and 
                (not self.get_keyword(key_val) or self.get_keyword(key_val) == 0)):
                keyflag = 0
            else:
                keyvars[key_i] = self.get_keyword(key_val)
            
        xdcal = keyvars[0]
        echcal = keyvars[1]
        xdsigmai = keyvars[2]
        xdangl = keyvars[3]
        echangl = keyvars[4]
            
        if keyflag == 1:
            #specifications, camera-collimator, blaze, cal offset angles
            camcol = 40.*np.pi/180.
            blaze = -4.38*np.pi/180.
            offset = 25.0-0.63 #0-order + 5deg for home

            #grating equation
            alpha = (xdangl+offset)*np.pi/180.
            d = 10.**7/xdsigmai #microns
            waveeq = lambda x,y,z: x*((1.+np.cos(y))*np.sin(z)-np.sin(y)*np.cos(z))
            wavecntr = waveeq(d,camcol,alpha)
            ccdangle = np.arctan2(npix*psize,(1.92*762.0)) #f = 762mm, psize u pix, 1.92 amag

            #blue end
            alphab = alpha - ccdangle
            waveblue = waveeq(d,camcol,alphab) 

            #red end
            alphar = alpha + ccdangle
            wavered = waveeq(d,camcol,alphar)

            #center, get non-decimal part of number
            wavecntr = np.fix(wavecntr)
            waveblue = np.fix(waveblue)
            wavered = np.fix(wavered)

            #get the correct equation constants
            if xdispers == 'RED':
                #order 62, 5748 A, order*wave = const
                const = 62.0 * 5748.0
                a = float(1.4088)
                b = float(-306.6910)
                c = float(16744.1946)
                
            if xdispers == 'UV':
                #order 97, 3677 A, order*wave = const
                const = 97.0 * 3677.0
                a = float(0.9496)
                b = float(-266.2792)
                c = float(19943.7496)
                
            if xdispers == '':
                return


            #correct wavelength values
            for i in [wavecntr,waveblue,wavered]:
                #find order for wavecntr
                wave = i
                order = np.floor(const/wave)
                trycount = 1
                okflag = 0
                while okflag == 0:
                    #find shift in Y: order 62 =npix with XD=ECH=0(red)
                    #                 order 97 =npix with XD=ECH=0(blue)
                    if i == wavecntr:
                        shift = a*order**2 + b*order + c
                        newy = npix - shift
                        newy = -newy
                        okflag = 1
                        
                    else:
                        shift2 = a*order**2 + b*order + c
                        
                        newy2 = shift2 - newy
                        if newy2 < 120:
                            order = order -1
                            trycount += 1
                            okflag=0
                            if trycount < 100:
                                continue
                        npix2 = 2*npix

                        if newy2 > npix2:
                            order = order + 1
                            trycount += 1
                            okflag=0
                            if trycount < 100:
                                continue
                        if trycount > 100 or newy2 > 120 or newy2 > npix2:
                            okflag=1
                            break

                #find delta wave for order
                dlamb = -0.1407*order + 18.005
                #new wavecenter = central wavelength for this order
                wave = const/order
                #correct for echangl
                wave = wave + (4*dlamb*echangl)
                #round to nearest 10 A
                wave2 = wave % 10 # round(wave,-1)
                if wave2 < 5:
                    wave = wave - wave2
                else:
                    wave = wave + (10-wave2)
                if wave < 1000 or wave > 20000:
                    wave = 'null'
                if i == wavecntr:
                    wavecntr = wave
                elif i == waveblue:
                    waveblue = wave
                elif i == wavered:
                    wavered = wave

            wavecntr = int(round(wavecntr,-1))
            waveblue = int(round(waveblue,-1))
            wavered = int(round(wavered,-1))
        else:
            wavecntr = 'null'
            waveblue = 'null'
            wavered = 'null'
            
        self.set_keyword('WAVECNTR',wavecntr,'Wave Center')
        self.set_keyword('WAVEBLUE',waveblue,'Wave Blue')
        self.set_keyword('WAVERED',wavered,'Wave Red')        

        return True


    def set_instr_status(self):
        '''
        Determine instrument status from instrument configuration

        inststat = 1 (good)
        inststat = 0 (abnormal)
        inststat = -1 (missing keywords)
        '''

        inststat = 1
        koaimtyp = self.get_keyword('IMAGETYP', default='')
        
        # Is this a bias/dark?
        dark = 0
        if koaimtyp == 'bias' or koaimtyp == 'dark': dark = 1
        
        keyList = ['CAFCAL', 'COFCAL', 'DECKCAL', 'ECHCAL', 'FIL1CAL', 'FIL2CAL',
                   'LFILCAL', 'LSELCAL', 'SLITCAL', 'TVACAL', 'TVFCAL', 'TVF1CAL',
                   'TVF2CAL', 'XDCAL', 'TEMPDET']
        
        # All keywords have to exist
        for key in keyList:
            if self.get_keyword(key) == None:
                inststat = -1
                self.set_keyword('INSTSTAT', inststat, 'KOA: HIRES instrument status')
                return True

        # Any with value of 0 is abnormal
        for key in keyList:
            if self.get_keyword(key, default='') == '0':
                inststat = 0
                self.set_keyword('INSTSTAT', inststat, 'KOA: HIRES instrument status')
                return True

        # Check detector temperature
        tempdet = self.get_keyword('TEMPDET')
        if (tempdet < -135 or tempdet > -115) and (tempdet < 32 and tempdet > 33):
            inststat = 0

        # Check the optics covers
        keyList1 = ['C1CVOPEN', 'C2CVOPEN', 'ECOVOPEN', 'XCOVOPEN']
        keyList2 = ['C1CVCLOS', 'C2CVCLOS', 'ECOVCLOS', 'XCOVCLOS']
        
        for i in range(0, 3):
            if self.get_keyword(keyList1[i]) == None or self.get_keyword(keyList2[i]) == None:
                inststat = -1
                self.set_keyword('INSTSTAT', inststat, 'KOA: HIRES instrument status')
                return True

        for i in range(0, 3):
            open = 0
            if self.get_keyword(keyList1[i]) == 1 and self.get_keyword(keyList2[i]) == 0:
                open = 1
            if not open and not dark: inststat = 0

        # Collimator
        keyList = ['XDISPERS', 'BCCVOPEN', 'BCCVCLOS', 'RCCVOPEN', 'RCCVCLOS']
        for key in keyList:
            if self.get_keyword(key) == None:
                inststat = -1
                self.set_keyword('INSTSTAT', inststat, 'KOA: HIRES instrument status')
                return True
        xdispers = self.get_keyword('XDISPERS')
        
        xd = {'RED':['RCCVOPEN', 'RCCVCLOS'], 'BLUE':['BCCVOPEN', 'BCCVCLOS']}

        open = 0
        hatopen = self.get_keyword('HATOPEN')
        hatclos = self.get_keyword('HATCLOS')
        if xdispers == 'RED':
            if self.get_keyword(xd[xdispers][0]) == 1 and self.get_keyword(xd[xdispers][1]) == 0:
                open = 1
            if not open and not dark: inststat = 0

            # Hatch
            if hatopen == None or hatclos == None:
                inststat = -1
        else:
            open = 0
            if hatopen == 1 and hatclos == 0: open = 1
            if not open and koaimtyp == 'object': inststat = 0

        self.set_keyword('INSTSTAT', inststat, 'KOA: HIRES instrument status')
        return True

    
    def set_slit_values(self):
        '''
        Determine slit scales from decker name
        '''
        slitlen = slitwidt = spatscal = specres = 'null'
        f15PlateScale = 0.7235 # mm/arcsec
        lambdaRes     = 5019.5 # A, res blaze order 71
        ccdSpaScale   = 0.1194 # arcsec/pixel - legacy 0.191
        ccdDispScale  = 0.1794 # arcsec/pixel - legacy 0.287
        dispRes       = 0.0219 # A/pixel, res blaze order 71 - legacy 0.035
        deckname = self.get_keyword('DECKNAME', default='')
        binning = self.get_keyword('BINNING', default='')
        xbin, ybin = binning.split(',')
        slitwid = self.get_keyword('SLITWID', default='')
        slitwid /= f15PlateScale
        spatscal = ccdSpaScale * int(xbin)
        dispscal = ccdDispScale * int(ybin)

        # Decker names
        decker = {}
        decker['A1'] =  [0.300, slitwid]
        decker['A2'] =  [0.500, slitwid]
        decker['A3'] =  [0.750, slitwid]
        decker['A4'] =  [1.000, slitwid]
        decker['A5'] =  [1.360, slitwid]
        decker['A6'] =  [1.500, slitwid]
        decker['A7'] =  [2.000, slitwid]
        decker['A8'] =  [2.500, slitwid]
        decker['A9'] =  [3.000, slitwid]
        decker['A10'] = [4.000, slitwid]
        decker['A11'] = [5.000, slitwid]
        decker['A12'] = [10.00, slitwid]
        decker['A13'] = [20.00, slitwid]
        decker['A14'] = [40.00, slitwid]
        decker['A15'] = [80.00, slitwid]
        decker['B1'] =  [3.500, 0.574]
        decker['B2'] =  [7.000, 0.574]
        decker['B3'] =  [14.00, 0.574]
        decker['B4'] =  [28.00, 0.574]
        decker['B5'] =  [3.500, 0.861]
        decker['C1'] =  [7.000, 0.861]
        decker['C2'] =  [14.00, 0.861]
        decker['C3'] =  [28.00, 0.861]
        decker['C4'] =  [3.500, 1.148]
        decker['C5'] =  [7.000, 1.148]
        decker['D1'] =  [14.00, 1.148]
        decker['D2'] =  [28.00, 1.148]
        decker['D3'] =  [7.000, 1.722]
        decker['D4'] =  [14.00, 1.722]
        decker['D5'] =  [0.119, 0.179]
        decker['E1'] =  [1.000, 0.400]
        decker['E2'] =  [3.000, 0.400]
        decker['E3'] =  [5.000, 0.400]
        decker['E4'] =  [7.000, 0.400]
        decker['E5'] =  [1.000, 0.400]

        # If decker exists, determine slit values
        if deckname in decker.keys():
            slitwidt = decker[deckname][1]
            slitlen = decker[deckname][0]
            prslwid = slitwidt / dispscal
            res = lambdaRes / (prslwid * dispRes * int(ybin))
            specres = res - (res % 100)
            slitlen = round(slitlen, 3)
            slitwidt = round(slitwidt, 3)
            specres = int(specres)
        else:
            main_logger.info('set_slit_values: Unable to set slit scale keywords')

        self.set_keyword('SLITLEN', slitlen, 'KOA: Slit length projected on sky (arcsec)')
        self.set_keyword('SLITWIDT', slitwidt, 'KOA: Slit width projected on sky (arcsec)')
        self.set_keyword('SPATSCAL', round(spatscal, 3), 'KOA: CCD pixel scale (arcsec/pixel)')
        self.set_keyword('SPECRES', specres, 'KOA: Nominal spectral resolution')
        self.set_keyword('DISPSCAL', round(dispscal, 3), 'KOA: CCD pixel scale, dispersion (arcsec/pixel)')

        return True
    
    
    def set_gain_and_rn(self): # ccdtype
        '''
        Assign values for CCD gain and read noise
        '''
        gain = {}
        gain['low']  = [1.20, 1.95, 1.13, 2.09, 1.26, 2.09]
        gain['high'] = [0.48, 0.78, 0.45, 0.84, 0.50, 0.89]
        readnoise = {}
        readnoise['low']  = [2.20, 2.90, 2.50, 3.00, 2.90, 3.10]
        readnoise['high'] = [2.00, 2.34, 2.50, 2.40, 2.60, 2.84]

        ccdgn = ['', 'null', 'null', 'null', 'null', 'null', 'null']
        ccdrn = ['', 'null', 'null', 'null', 'null', 'null', 'null']

        ccdgain = self.get_keyword('CCDGAIN')
        for ext in range(1, len(self.fits_hdu)):
            ccdgain = ccdgain.strip()
            amploc  = self.fits_hdu[ext].header['AMPLOC']
            if amploc != None and ccdgain != None:
                amploc = int(amploc.strip()) - 1

                ccdgn[ext] = gain[ccdgain][amploc]
                ccdrn[ext] = readnoise[ccdgain][amploc]

        self.set_keyword('CCDGN01', ccdgn[1], 'KOA: CCD gain extension 1')
        self.set_keyword('CCDGN02', ccdgn[2], 'KOA: CCD gain extension 2')
        self.set_keyword('CCDGN03', ccdgn[3], 'KOA: CCD gain extension 3')
        self.set_keyword('CCDGN04', ccdgn[4], 'KOA: CCD gain extension 4')
        self.set_keyword('CCDGN05', ccdgn[5], 'KOA: CCD gain extension 5')
        self.set_keyword('CCDGN06', ccdgn[6], 'KOA: CCD gain extension 6')
        self.set_keyword('CCDRN01', ccdrn[1], 'KOA: CCD read noise extension 1')
        self.set_keyword('CCDRN02', ccdrn[2], 'KOA: CCD read noise extension 2')
        self.set_keyword('CCDRN03', ccdrn[3], 'KOA: CCD read noise extension 3')
        self.set_keyword('CCDRN04', ccdrn[4], 'KOA: CCD read noise extension 4')
        self.set_keyword('CCDRN05', ccdrn[5], 'KOA: CCD read noise extension 5')
        self.set_keyword('CCDRN06', ccdrn[6], 'KOA: CCD read noise extension 6')

        return True
    
    
    def set_skypa(self): # skypa
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
            main_logger.info('set_skypa: Could not set skypa')
            return True

        skypa = (2.0 * float(irot2ang) + float(parang) + float(el) + offset) % (360.0)
        self.set_keyword('SKYPA', round(skypa, 4), 'KOA: Position angle on sky (deg)')
        return True
    
    
    def set_subexp(self):
        '''
        Determine if file is part of a subexposure sequence
        '''
        subexp = 'False'
        comment = ''

        # PEXPTIME and PEXPELAP == 0 for a regular exposure
        pexptime = self.get_keyword('PEXPTIME', default=0)
        pexpelap = self.get_keyword('PEXPELAP', default=0)
        if pexptime != 0 or pexpelap != 0:
            eramode = self.get_keyword('ERAMODE', default='')
            mosmode = self.get_keyword('MOSMODE', default='')
            if eramode != mosmode or eramode.replace(" ", "").strip() != 'B,G,R':
                # Stage directory listing
                outdir = self.get_keyword('OUTDIR')
                ofname = self.get_keyword('OFNAME')
                dir = self.dirs['stage'] + outdir

                # Find OFNAME
                cmd = 'ls ' + dir + '*.fits'
                cmdres = os.system(cmd)
                i = np.where(cmdres == file)

                # Find the start of the sequence
                j = i.copy()
                while j > 0:
                    fitsfile = fits.open(cmdres[j])
                    eramode2 = fitsfile.Hdu[0].header['ERAMODE']
                    mosmode2 = fitsfile.Hdu[0].header['MOSMODE']
                    if (j != i and eramode2 != mosfmode2) or (mosmode2 == 'B,G,R'):
                        first = j
                        break
                    mosmode = mosmode2
                    eramode = eramode2
                    j -= 1

                # Find the end of the sequence
                j = i.copy()
                while j < len(cmdres):
                    fitsfile = fits.open(cmdres[j])
                    eramode2 = fitsfile.Hdu[0].header['ERAMODE']
                    mosmode2 = fitsfile.Hdu[0].header['MOSMODE']
                    if (j != i and eramode2 != mosmode) or (mosmode2 == 'B,G,R'):
                        last = j
                        break
                    mosmode = mosmode2
                    eramode = eramode2
                    j += 1

                if first != last:
                    subexp = 'True'
                    num1 = i - first + 1
                    num2 = last - first + 1
                    comment = ' (' + str(num1) + ' of ' + str(num2) + ')'

        self.set_keyword('SUBEXP',  subexp,   'KOA: Sub-exposure' + comment)

        return True


    def set_roqual(self):
        '''
        Determine if an electronic glitch has compromised data
        Just setting to Good since the glitch has been fixed since 20110421
        '''

        self.set_keyword('ROQUAL',  'Good',   'KOA: Postscan row quality')

        return True
 

    def set_image_stats(self):
        '''
        Adds mean, median, std keywords to header
        '''

        precol  = self.get_keyword('PRECOL')
        postpix = self.get_keyword('POSTPIX')
        binning = self.get_keyword('BINNING')
        bin = binning.split(',')
        xbin = int(bin[0])
        ybin = int(bin[1])

        # Size of sampling box 15x15
        nx = postpix / xbin
        nx = nx - nx / 3
        if nx > 15: nx = 15
        ny = nx

        # Can be up to 6 extensions
        for ext in range(1, 7):
            imageStd = imageMean = imageMedian = 'null'
            postStd = postMean = postMedian = 'null'
            if ext < len(self.fits_hdu):
                image = self.fits_hdu[ext].data

                # Rotate so same IDL equations work
                image = np.rot90(image, 3)
                naxis1 = self.fits_hdu[ext].header['NAXIS1']
                naxis2 = self.fits_hdu[ext].header['NAXIS2']

                # image pixels and start of postscan
                nxi = naxis1 - postpix / xbin - precol / xbin
                px1 = precol / xbin + nxi - 1

                # Center of image and postcan
                cxi = precol / xbin + nxi / 2
                cyi = naxis2 / 2
                cxp = px1 + postpix / xbin / 2

                # Image area
                x1 = int(cxi-nx/2)
                x2 = int(cxi+nx/2)
                y1 = int(cyi-ny/2)
                y2 = int(cyi+ny/2)
                img = image[x1:x2, y1:y2]
                imageStd    = float("%0.2f" % np.std(img))
                imageMean   = float("%0.2f" % np.mean(img))
                imageMedian = float("%0.2f" % np.median(img))

                # postscan area
                x1 = int(cxp-nx/2)
                x2 = int(cxp+nx/2)
                y1 = int(naxis2*0.03-ny/2)
                y2 = int(naxis2*0.03+ny/2)
                img = image[x1:x2, y1:y2]
                postStd    = float("%0.2f" % np.std(img))
                postMean   = float("%0.2f" % np.mean(img))
                postMedian = float("%0.2f" % np.median(img))

            key = str(ext).zfill(2)
            key_mn = 'IM01MN' + key
            key_md = 'IM01MD' + key
            key_sd = 'IM01SD' + key
            self.set_keyword(key_mn,  imageMean,   'KOA: Image data mean')
            self.set_keyword(key_sd,  imageStd,    'KOA: Image data standard deviation')
            self.set_keyword(key_md,  imageMedian, 'KOA: Image data median')
            key_mn = 'PT01MN' + key
            key_md = 'PT01MD' + key
            key_sd = 'PT01SD' + key
            self.set_keyword(key_mn,  postMean,   'KOA: Postscan data mean')
            self.set_keyword(key_sd,  postStd,    'KOA: Postscan data standard deviation')
            self.set_keyword(key_md,  postMedian, 'KOA: Postscan data median')

        return True


    def get_numamps(self):
        '''
        Determine number of amplifiers
        '''

        ampmode = self.get_keyword('AMPMODE', default='')
        if 'DUAL:A+B' in ampmode: numamps = 2
        elif ampmode == '':       numamps = 0
        else:                     numamps = 1

        return numamps


    def set_sig2nois(self):
        '''
        Calculates S/N for middle CCD image
        '''
        numamps = self.get_numamps()

        # Middle extension
        ext = floor(len(self.fits_hdu)/2.0)
        image = self.fits_hdu[ext].data

        naxis1 = self.fits_hdu[ext].header['NAXIS1']
        naxis2 = self.fits_hdu[ext].header['NAXIS2']
        postpix = self.get_keyword('POSTPIX', default=0)
        precol = self.get_keyword('PRECOL', default=0)

        nx = (naxis2 - numamps * (precol + postpix))
        c = [naxis1 / 2, 1.17 * nx / 2]

        wsize = 10
        spaflux = []
        for i in range(wsize, int(naxis1)-wsize):
            spaflux.append(np.median(image[int(c[1])-wsize:int(c[1])+wsize, i]))

        maxflux = np.max(spaflux)
        minflux = np.min(spaflux)

        sig2nois = np.fix(np.sqrt(np.abs(maxflux - minflux)))

        self.set_keyword('SIG2NOIS', sig2nois, 'KOA: S/N estimate near image spectral center')

        return True


    def fix_propint(self):
        '''
        HIRES needs PROPINT1, 2, 3 and PROPMIN
        '''
        if 'PROPINT' in self.extra_meta.keys():
            self.extra_meta['PROPINT1'] = self.extra_meta['PROPINT']
            self.extra_meta['PROPINT2'] = self.extra_meta['PROPINT']
            self.extra_meta['PROPINT3'] = self.extra_meta['PROPINT']
            self.extra_meta['PROPMIN'] = self.extra_meta['PROPINT']

        return True


    def make_jpg(self):
        '''
        Converts HIRES FITS file to JPG image
        Output filename = KOAID_CCD#_HDU##.jpg
            # = 1, 2, 3...
            ## = 01, 02, 03...
        '''

        # TODO: Can we utilize instrument.make_jpg() to reduce duplicate code?  
        # Perhaps add an 'ext' param to make_jpg().

        # file to convert is lev0Dir/KOAID

        koaid = self.get_keyword('KOAID')
        filePath = ''
        for root, dirs, files in os.walk(self.dirs['lev0']):
            if koaid in files:
                filePath = ''.join((root, '/', koaid))
        if not filePath or not os.path.isfile(filePath):
            self.log_warn('MAKE_JPG_ERROR')
            return False

        koaid = filePath.replace('.fits', '')
        for ext in range(1, len(self.fits_hdu)):
            try:
                ext2 = str(ext)
                pngFile = koaid+'_CCD'+ext2+'_HDU'+ext2.zfill(2)+'.png'
                jpgFile = pngFile.replace('.png', '.jpg')
                # image data to convert
                image = self.fits_hdu[ext].data
                interval = ZScaleInterval()
                vmin, vmax = interval.get_limits(image)
                norm = ImageNormalize(vmin=vmin, vmax=vmax, stretch=AsinhStretch())
                fig = plt.figure()
                ax = plt.axes([0, 0, 1, 1])
                ax.get_xaxis().set_visible(False)
                ax.get_yaxis().set_visible(False)
                plt.imshow(np.rot90(image), cmap='gray', origin='lower', norm=norm)
                plt.axis('off')
                # save as png, then convert to jpg
                plt.savefig(pngFile, bbox_inches='tight', pad_inches=0)
                img = Image.open(pngFile).convert('RGB')
                basewidth = int(len(image)/2)
                wpercent = basewidth/float(img.size[0])
                hsize = int((float(img.size[1]) * float(wpercent)))
                img = img.resize((basewidth, hsize), Image.ANTIALIAS)
                img.save(jpgFile)
                os.remove(pngFile)
                plt.close()
            except:
                self.log_warn("MAKE_JPG_ERROR", jpgFile)
                return False

        return True


    def set_npixsat(self, satVal=None):
        '''
        Determines number of saturated pixels and adds NPIXSAT to header
        NPIXSAT is the sum of all image extensions.
        '''
        if satVal == None:
            satVal = self.get_keyword('SATURATE')
        if satVal == None:
            self.log_warn("SET_NPIXSAT_ERROR", "No saturate value")
            return False

        nPixSat = 0
        for ext in range(1, len(self.fits_hdu)):
            image = self.fits_hdu[ext].data
            pixSat = image[np.where(image >= satVal)]
            nPixSat += len(image[np.where(image >= satVal)])
        self.set_keyword('NPIXSAT', nPixSat, 'KOA: Number of saturated pixels')
        return True


    def set_utend(self):
        '''
        Create UT-END keyword from UTC-END
        '''

        #try to get from header unmapped and mark if update needed
        utc = self.get_keyword('UTC-END')
        if utc == None: return True

        self.set_keyword('UT-END', utc, 'KOA: Duplicate of UTC-END')

        return True


    def has_target_info(self):
        '''
        Does this fits have sensitive target info?
        '''
        return False

