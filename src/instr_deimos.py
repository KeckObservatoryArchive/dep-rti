"""
This is the class to handle all the DEIMOS specific attributes
"""

import instrument
import datetime as dt
from common import *
import numpy as np
from astropy.io import fits

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from astropy.visualization import ZScaleInterval, AsinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize


class Deimos(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None, logger_name=DEFAULT_LOGGER_NAME):

        super().__init__(instr, filepath, reprocess, transfer, progid, dbid, logger_name)

        # Set any unique keyword index values here
        self.keymap['OFNAME']   = 'OUTFILE'

        # Other vars that subclass can overwrite
        # Skip warnings for these FCS-only keywords
        self.keyskips   = ['EXPOSURE', 'MPPMODE', 'NAXIS1', 'NAXIS2']
        self.keyskips.extend(['NUMAMPS', 'OBSNUM', 'PREPIX', 'SFRAMENO'])
        self.keyskips.extend(['SHUTSTAT', 'SOBJECT', 'SOBSTYPE', 'SOUTDIR'])
        self.keyskips.extend(['SOUTFILE', 'STTIME', 'SYNOPFMT', 'SYNOPSIS'])
        self.keyskips.extend(['TODISK', 'VOFFSET0', 'VOFFSET1', 'WINDOW', 'YFLIP'])


        # """
        # Values not included in superclass, specific to DEIMOS
        # """
        # # add the FCSIMGFI config file for deimos
        # self.fcsimgfi = 'FCSIMGFI'
        self.gratingList = {}
        self.gratingList['600ZD'] = {'wave':7500, 'dispersion':0.65, 'length':5300}
        self.gratingList['830G']  = {'wave':8640, 'dispersion':0.47, 'length':3840}
        self.gratingList['900ZD'] = {'wave':5500, 'dispersion':0.44, 'length':3530}
        self.gratingList['1200G'] = {'wave':7760, 'dispersion':0.33, 'length':2630}
        self.gratingList['1200B'] = {'wave':4500, 'dispersion':0.33, 'length':2630}
        # Filter list for imaging wavelengths
        self.filterList = {}
        self.filterList['B']      = {'blue':4200, 'cntr':4400, 'red':4600}
        self.filterList['V']      = {'blue':5150, 'cntr':5450, 'red':5750}
        self.filterList['R']      = {'blue':6100, 'cntr':6500, 'red':6900}
        self.filterList['I']      = {'blue':7600, 'cntr':8400, 'red':9200}
        self.filterList['Z']      = {'blue':8600, 'cntr':9100, 'red':9600}
        self.filterList['GG400']  = {'blue':4000, 'cntr':7250, 'red':10500}
        self.filterList['GG455']  = {'blue':4550, 'cntr':7525, 'red':10500}
        self.filterList['GG495']  = {'blue':4950, 'cntr':7725, 'red':10500}
        self.filterList['OG550']  = {'blue':5500, 'cntr':8000, 'red':10500}
        self.filterList['NG8560'] = {'blue':8400, 'cntr':8550, 'red':8700}
        self.filterList['NG8580'] = {'blue':8550, 'cntr':8600, 'red':8650}


    def run_dqa(self):
        '''Run all DQA check unique to this instrument'''

        funcs = [
            {'name':'set_telnr',        'crit': True},
            {'name':'set_fcs_date_time','crit': True},
            {'name':'set_ut',           'crit': True},
            {'name':'set_koaimtyp',     'crit': True},
            {'name':'set_fcskoaid',     'crit': False},
            {'name':'set_ofName',       'crit': True},
            {'name':'set_semester',     'crit': True},
            {'name':'set_prog_info',    'crit': True},
            {'name':'set_weather',      'crit': False},
            {'name':'set_oa',           'crit': False},
            {'name':'set_camera',       'crit': True},
            {'name':'set_filter',       'crit': True},
            {'name':'set_mjd',          'crit': False},
            {'name':'set_obsmode',      'crit': False},
            {'name':'set_nexten',       'crit': False},
            {'name':'set_detsec',       'crit': False},
            {'name':'set_npixsat',      'crit': False,  'args': {'satVal':65535.0}},
            {'name':'set_wavelengths',  'crit': False},
            {'name':'set_spatscal',     'crit': False},
            {'name':'set_dispscal',     'crit': False},
            {'name':'set_specres',      'crit': False},
            {'name':'set_propint',      'crit': True},
            {'name':'set_datlevel',     'crit': False,  'args': {'level':0}},
            {'name':'set_dqa_vers',     'crit': False},
            {'name':'set_dqa_date',     'crit': False},
        ]
        return self.run_functions(funcs)


    def get_dir_list(self):
        """
        Function to generate the paths to all the DEIMOS accounts, including engineering
        Returns the list of paths
        """
        dirs = []
        path = '/s/sdata100'
        for i in range(1,6):
            path2 = path + str(i)
            for j in range(1,21):
                path3 = path2 + '/deimos' + str(j)
                dirs.append(path3)
            path3 = path2 + '/dmoseng'
            dirs.append(path3)
        return dirs


    def get_prefix(self):
        '''
        Returns the KOAID prefix to use, either DE for a DEIMOS science/calibration
        exposure or DF for an FCS image.
        '''

        instr = self.get_instr()
        outdir = self.get_keyword('OUTDIR')
        
        if '/fcs' in outdir:
            prefix = 'DF'
        elif instr == 'deimos':
            prefix = 'DE'
        else:
            prefix = ''
        return prefix


    def set_fcs_date_time(self):
        '''
        For FCS images, set DATE-OBS and UTC from DATE
        '''

        if '/fcs' not in self.get_keyword('OUTDIR', default=''):
            return True

        dateVal = self.get_keyword('DATE', default='')
        if 'T' not in dateVal:
            return True

        dateobs, utc = dateVal.split('T')
        utc = f'{utc}.00'
        
        self.logger.info('set_fcs_date_time: Setting DATE-OBS and UTC from DATE')
        self.set_keyword('DATE-OBS', dateobs, 'KOA: Created from DATE keyword value')
        self.set_keyword('UTC', utc, 'KOA: Created from DATE keyword value')
        
        return True


    def set_ofName(self):
        '''
        Sets OFNAME keyword from OUTFILE and FRAMENO
        '''
        outfile = self.get_keyword('OUTFILE', False)
        frameno = self.get_keyword('FRAMENO', False)
        if outfile == None or frameno == None:
            self.log_error('SET_OFNAME_ERROR')
            return False
        
        frameno = str(frameno).zfill(4)
        ofName = ''.join((outfile, frameno, '.fits'))
        self.set_keyword('OFNAME', ofName, 'KOA: Original file name')

        return True


    def set_koaimtyp(self):
        '''
        Calls get_koaimtyp to determine image type. 
        Creates KOAIMTYP keyword.
        '''

        if '/fcs' in self.get_keyword('OUTDIR', default=''):
            koaimtyp = 'fcscal'
        else:
            koaimtyp = self.get_koaimtyp()

        # Warn if undefined
        if koaimtyp == 'undefined':
            self.logger.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")

        # Create the keyword
        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')

        return True


    def get_koaimtyp(self):
        '''
        Return image type based on the algorithm provided by SA
        '''

        # Get relevant keywords from header
        obstype  = self.get_keyword('OBSTYPE', default='').lower()
        slmsknam = self.get_keyword('SLMSKNAM', default='').lower()
        hatchpos = self.get_keyword('HATCHPOS', default='').lower()
        flimagin = self.get_keyword('FLIMAGIN', default='').lower()
        flspectr = self.get_keyword('FLSPECTR', default='').lower()
        lamps    = self.get_keyword('LAMPS', default='').lower()
        gratepos = self.get_keyword('GRATEPOS')

        # if obstype is 'bias' we have a bias
        if obstype == 'bias':
            return 'bias'

        # if obsmode is 'dark' we have a dark
        if obstype == 'dark':
            return 'dark'

        # if slmsknam contains 'goh' we have a focus image
        if slmsknam.startswith('goh'):
            return 'focus'

        # if hatch is closed and lamps are quartz, then flat
        if hatchpos == 'closed' and 'qz' in lamps:
            return 'flatlamp'

        # if hatch is open and flimagin or flspectr are on, then flat
        if hatchpos == 'open' and (flimagin == 'on' or flspectr == 'on'):
            return 'flatlamp'

        # if lamps are not off/qz and grating position is 3 or 4, then arc
        if hatchpos == 'closed' and ('off' not in lamps and 'qz' not in lamps)\
           and (gratepos == 3 or gratepos == 4):
            return 'arclamp'

        # if tracking then we must have an object science image
        # otherwise, don't know what it is
        if hatchpos == 'open':
            return 'object'

        # check for fcs image
        outdir = self.get_keyword('OUTDIR', default='')
        if 'fcs' in outdir:
            return 'fcscal'

        return 'undefined'


    def set_camera(self):
        '''
        Adds the keyword CAMERA to the header and sets its value to DEIMOS
        '''
        
        camera = self.get_keyword('CAMERA', False)
        if camera == None:
            self.logger.info('set_camera: Adding CAMERA keyword')
            self.set_keyword('CAMERA', 'DEIMOS', 'KOA: Camera name')

        return True


    def set_filter(self):
        '''
        Adds the keyword FILTER to the header and sets its value to be the 
        same as the DWFILNAM keyword.
        '''

        filter = self.get_keyword('DWFILNAM', False)
        if filter == None:
            self.logger.info('set_filter: Could not set filter, no DWFILNAM value')
        else:
            self.set_keyword('FILTER', filter, 'KOA: Filter name')
        return True


    def set_mjd(self):
        '''
        Adds the keyword MJD to the header and sets its value equal to the 
        MJD-OBS keyword.  MJD is the numeric respresentation of MJD-OBS.
        '''

        mjd = self.get_keyword('MJD-OBS', False)
        if mjd == None:
            self.logger.info('set_mjd: Could not set MJD, no MJD-OBS value')
        else:
            self.set_keyword('MJD', float(mjd), 'KOA: Modified julian day')
        return True


    def set_obsmode(self):
        '''
        Adds the keyword OBSMODE to the header.
        
        UNKNOWN if:  GRATENAM = "Unknown", "None", or (blank)
        IMAGING if:  GRATENAM = "Mirror"
                     OR
                     ( GRATEPOS = 3 and G3TLTNAM = "Zeroth_Order"
                       OR
                       GRATEPOS = 4 and G4TLTNAM = "Zeroth_Order"
                     )
        LONGSLIT if: GRATENAM != ["Mirror", "Unknown", "None", (blank)]
                     AND
                     SLMSKNAM contains "LVM*" or "Long*"
        MOS if:      GRATENAM != ["Mirror", "Unknown", "None", (blank)]
                     AND
                     SLMSKNAM != ["LVM*", "Long*"]
        '''        
        obsmode = 'null'
        
        gratname = self.get_keyword('GRATENAM', default='').lower()
        gratepos = self.get_keyword('GRATEPOS', default=0)

        if gratname in ['', 'unknown', 'none']:
            obsmode = 'unknown'
        elif gratname == 'mirror':
            obsmode = 'imaging'
        elif int(gratepos) == 3 or int(gratepos) == 4:
            key = f'G{int(gratepos)}TLTNAM'
            tilt = self.get_keyword(key, default='').lower()
            if tilt == 'zeroth_order':
                obsmode = 'imaging'
#            else:
#                obsmode = 'imaging'
            else:
                slmsknam = self.get_keyword('SLMSKNAM', default='')
                if slmsknam.startswith('LVM') or slmsknam.startswith('Long'):
                    obsmode = 'longslit'
                else:
                    obsmode = 'mos'

        self.set_keyword('OBSMODE', obsmode, 'KOA: Observing mode')

        return True


    def set_nexten(self):
        '''
        Adds the NEXTEN keyword and sets its value to the number of
        imaging extensions for this file.
        '''
        self.set_keyword('NEXTEN', int(len(self.fits_hdu))-1, 'KOA: Number of image extensions')
        return True


    def set_detsec(self):
        '''
        Adds the DETSEC## keywords to the primary header.  Value of the
        keyword is set equal to the DETSEC keyword value from the image
        headers.  ## = 01 to 16.
        '''
        maxExtensions = 16
        for i in range(1, maxExtensions+1):
            key = f'DETSEC{str(i).zfill(2)}'
            detsec = 'null'
            if i < len(self.fits_hdu):
                try:
                    detsec = self.fits_hdu[i].header['DETSEC']
                except:
                    pass
            comment = f'KOA: Mosaic detector section for HDU{str(i).zfill(2)}'
            self.set_keyword(key, detsec, comment)

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
            if 'ndarray' not in str(type(image)): continue
            pixSat = image[np.where(image >= satVal)]
            nPixSat += len(image[np.where(image >= satVal)])
        self.set_keyword('NPIXSAT', nPixSat, 'KOA: Number of saturated pixels')
        return True


    def set_wavelengths(self):
        '''
        Adds wavelength keywords.
        NOTE: Dependent on set_obsmode
        '''

        waveblue = wavecntr = wavered = 'null'

        # Is this an image or spectrum?
        obsmode = self.get_keyword('OBSMODE')
        if obsmode == 'imaging':
            filter = self.get_keyword('FILTER', default='').strip()
            if filter in self.filterList.keys():
                waveblue = self.filterList[filter]['blue']
                wavecntr = self.filterList[filter]['cntr']
                wavered  = self.filterList[filter]['red']

        elif obsmode in ['longslit', 'mos']:
            gratepos = self.get_keyword('GRATEPOS')
            waveKey = f'G{gratepos}TLTWAV'
            grating = self.get_keyword('GRATENAM')
            if grating in self.gratingList.keys():
                wavecntr = int(round(self.get_keyword(waveKey), -1))
                delta = self.gratingList[grating]['length']/2
                waveblue = int(round(wavecntr - delta, -1))
                wavered = int(round(wavecntr + delta, -1))

        else:
            pass

        self.set_keyword('WAVEBLUE', waveblue, 'KOA: Blue end wavelength')
        self.set_keyword('WAVECNTR', wavecntr, 'KOA: Center wavelength')
        self.set_keyword('WAVERED' , wavered, 'KOA: Red end wavelength')

        return True


    def set_spatscal(self):
        '''
        Populates SPATSCAL
        '''
        
        self.set_keyword('SPATSCAL', 0.1185, 'KOA: CCD spatial pixel scale')
        return True


    def set_dispscal(self):
        '''
        Populates DISPSCAL
        NOTE: Dependent on set_obsmode
        '''

        dispscal = 'null'
        com = ''

        obsmode = self.get_keyword('OBSMODE')
        spatscal = self.get_keyword('SPATSCAL')
        if obsmode == 'imaging':
            dispscal = spatscal
            com = ' (arcsec/pix)'
        elif obsmode in ['longslit', 'mos']:
            grating = self.get_keyword('GRATENAM')
            if grating in self.gratingList.keys():
                dispscal = self.gratingList[grating]['dispersion']
                com = ' (A/pix)'

        self.set_keyword('DISPSCAL', dispscal, f'KOA: CCD dispersion pixel scale{com}')
        return True


    def set_specres(self):
        '''
        Calculates the spectral resolution and add SPECRES to header
        '''

        specres = 'null'

        spatscal = self.get_keyword('SPATSCAL')
        grating = self.get_keyword('GRATENAM')

        if grating in self.gratingList.keys():
            specres = (self.gratingList[grating]['wave']*spatscal)/self.gratingList[grating]['dispersion']
            specres = round(specres, -1)

        self.set_keyword('SPECRES', specres, 'KOA: nominal spectral resolution')

        return True


    def create_jpg_from_fits(self, fits_filepath, outdir):
        '''
        Overriding instrument default function
        Tile images horizontally in order from left to right.
        Use DETSEC keyword to figure out data order/position
        '''

        # Only create jpeg for level 1 processing
        if self.level != 1:
            return
        
        #open
        hdus = fits.open(fits_filepath, ignore_missing_end=True)

        #needed hdr vals
        hdr0 = hdus[0].header

        if hdr0['KOAID'].startswith('DF'):
            super().create_jpg_from_fits(fits_filepath, outdir)
            return
            
        binning  = hdr0['BINNING'].split(',')
        precol   = int(hdr0['PRECOL'])   // int(binning[0])
        postpix  = int(hdr0['POSTPIX'])  // int(binning[0])
        preline  = int(hdr0['PRELINE'])  // int(binning[1])
        postline = int(hdr0['POSTLINE']) // int(binning[1])

        #get extension order (uses DETSEC keyword)
        ext_order = Deimos.get_ext_data_order(hdus)
        assert ext_order, "ERROR: Could not determine extended data order"

        #loop thru extended headers in order, create png and add to list in order
        interval = ZScaleInterval()
        vmin = None
        vmax = None
#        alldata = None
        # DEIMOS has 2 rows of 4 CCDs each
        alldata = [[], []]
        for row, extData in enumerate(ext_order):
            if len(extData) == 0: continue
            for i, ext in enumerate(extData):
                data = hdus[ext].data
                hdr  = hdus[ext].header
                if 'ndarray' not in str(type(data)): continue

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
                ds = Deimos.get_detsec_data(hdr['DETSEC'])
                if ds and ds[0] > ds[1]:
                    data = np.fliplr(data)
                if ds and ds[2] > ds[3]:
                    data = np.flipud(data)

                #concatenate horizontally
                if i==0:
                    alldata[row] = data
                else   :
                    alldata[row] = np.append(alldata[row], data, axis=1)

        # If alldata has 2 rows, then vertically stack them
        # else take the one row

        s0 = len(alldata[0])
        s1 = len(alldata[1])

        if s0 > 0 and s1 > 0:
            alldata = np.concatenate((alldata[0], alldata[1]), axis=0)
            # Need to rotate final stitched image
#            alldata = ndimage.rotate(alldata, -90, axes=(0, 1))
            alldata = np.rot90(alldata, 1, axes=(1, 0))
        elif s0 > 0:
            alldata = alldata[0]
        elif s1 > 0:
            alldata = alldata[1]

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
        # DEIMOS jpegs are large, let's reduce the size using dpi (default is 100)
        plt.savefig(out_filepath, quality=92, dpi=(50))
        plt.close()


    @staticmethod
    def get_ext_data_order(hdus):
        '''
        Use DETSEC keyword to figure out true order of extension data for horizontal tiling
        '''
        key_orders = [{}, {}]
        for i in range(1, len(hdus)):
            arrRow = 0
            try:
                ds = Deimos.get_detsec_data(hdus[i].header['DETSEC'])
                if not ds: return None
                if ds[2] > 4096 or ds[3] > 4096: arrRow = 1
                key_orders[arrRow][ds[0]] = i
            except:
                pass

        # DEIMOS can have 2 rows of 4 CCDs each
        orders = [[], []]
        for num, entry in enumerate(key_orders):
            for key in sorted(entry):
                orders[num].append(entry[key])

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


    def set_fcskoaid(self):
        '''
        Populates FCSKOAID with the associated FCS file
        '''
        fcskoaid = ''
        fcs = self.get_keyword('FCSIMGFI', default='')
        if fcs:
            if fcs.startswith('/sdata'): fcs = f"/s{fcs}"
            query = f"select * from koa_status where level=0 and ofname='{fcs}' order by id desc limit 1"
            row = self.db.query('koa', query, getOne=True)
            if row:
                fcskoaid = row['koaid']
                if fcskoaid == None: return True                
                if not fcskoaid.endswith('.fits'):
                    fcskoaid = f"{fcskoaid}.fits"

        self.set_keyword('FCSKOAID', fcskoaid, 'KOA: associated fcs file')
        return True


    def has_target_info(self):
        '''
        Does this fits have target info?
        If any header name is not PrimaryHDU or ImageHDU, then yes.
        '''
        has_target = False
        non_target_names = ['PrimaryHDU', 'ImageHDU']
        for ext in range(0, len(self.fits_hdu)):
            if not any(x in str(type(self.fits_hdu[ext])) for x in non_target_names):
                has_target = True
        return has_target


    def get_drp_files_list(self, datadir, koaid, level):
        '''
        Return list of files to archive for DRP specific to DEIMOS.

        QL ingest (KOA level 1)
            icubed.fits files
            icubes.fits files
            calibration validation (arc_ and bars_ < FRAMENO)

        Science ingest (KOA level 2)
            Science/*KOAID*.[fits|txt]
            QA/PNGs/KOAI*.png and associated Arc*.png files
            Associated Masters/Master*.fits
            calib, log and pytpeit files
        '''
        files = []

        #back out of /redux/ subdir
        if datadir.endswith('/'): datadir = datadir[:-1]
        maskConfig = datadir.split('/')[-1]

        #level 1 and greater
        if level >= 1:
            searchfiles = []
            for f in searchfiles:
                if os.path.isfile(f): files.append(f)

        #level 2 (note: includes level 1 stuff, see above)
        if level == 2:
            searchfiles = [
                f"{datadir}/{maskConfig}.calib",
                f"{datadir}/{maskConfig}.log",
                f"{datadir}/{maskConfig}.pypeit"
            ]
            for f in searchfiles:
                if os.path.isfile(f): files.append(f)
            # Search for all files with a matching koaid
            associatedFiles = []
            for rootdir, dirs, fileList in os.walk(datadir):
                for file in fileList:
                    if koaid in file:
                        files.append(os.path.join(rootdir, file))
                    if file.startswith('Master') or file.startswith('Arc'):
                        associatedFiles.append(os.path.join(rootdir, file))

            # Now search for all associated files
            for file in files:
                start = file.find('DET0')
                end   = file.find('_', start)
                masterSearch = file[start:end]
                end   = file.find('_', start+6)
                arcSearch = file[start:end]
                # Search for all master calibrations and arc QA plots for this detector
                for afile in associatedFiles:
                    if '/Master' in afile and masterSearch in afile:
                        if afile not in files:
                            files.append(afile)
                    if '/Arc' in afile and arcSearch in afile:
                        if afile not in files:
                            files.append(afile)

        return files


    def get_drp_destfile(self, koaid, srcfile):
        '''
        Returns the destination of the DRP file for RTI.
        '''

        # PypeIt uses 'keck_deimos_X'
        loc = srcfile.find('keck_deimos')

        # New desitnation file
        outdir = self.dirs[f'lev{self.level}']
        destfile = f"{outdir}/{srcfile[loc:]}"

        return True, destfile


    def run_lev1(self):
        '''
        Create a stitched together, trimmed and bias subtracted JPEG image of
        the FITS data.
        '''

        outdir = self.dirs[f'lev{self.level}']
        # If lev0 is calling this, then create a database entry
        if self.level == 0:
            stage_file = f"{outdir}/{self.koaid}.fits"
            if self.reprocess:
                return
            query = ("insert into koa_status set "
                    f"instrument='{self.instr}',"
                    f"level=1,"
                    f"koaid='{self.koaid}',"
                    f"service='RTI',"
                    f"status='QUEUED',"
                    f"stage_file='{stage_file}',"
                    f"creation_time='{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}' ")
            self.logger.info(query)
            result = self.db.query('koa', query)
            if result is False:
                self.log_warn('LEV1_INSERT_ERROR', query)

            return True

        # Else create the jpeg file
        self.drp_files={}
        self.drp_files[self.koaid] = []
        if self.status['service'] != 'RTI':
            return True
        fits_filepath = self.status['stage_file']
        self.create_jpg_from_fits(fits_filepath, outdir)
        # Verify jpg was created
        jpg_file = f"{outdir}/{self.koaid}.jpg"
        if os.path.exists(jpg_file):
            self.drp_files[self.koaid].append(jpg_file)
            self.xfr_files.append(jpg_file)
        return True
