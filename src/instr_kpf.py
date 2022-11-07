"""
This is the class to handle all the HIRES specific attributes
"""
import os
import glob
import warnings
import instrument
from common import *
import numpy as np
from astropy.io import fits
from astropy.visualization import (SqrtStretch, ImageNormalize, ZScaleInterval)
from datetime import datetime
import matplotlib.pyplot as plt

import logging
log = logging.getLogger('koa_dep')


class Kpf(instrument.Instrument):

    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        self.dev = False
        self.progid = progid
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)

    def run_dqa(self):
        """
        Run all DQA checks unique to this instrument.
        """
        funcs = [
            {'name': 'set_telnr', 'crit': True},
            {'name': 'set_inst', 'crit': True},
            {'name': 'set_semester', 'crit': True},
            {'name': 'set_elaptime', 'crit': True},
            {'name': 'set_koaimtyp', 'crit': True},
            {'name': 'set_prog_info', 'crit': True},
            {'name': 'set_propint', 'crit': True},

            {'name': 'set_weather', 'crit': False},
            # {'name': 'set_obsmode', 'crit': False},
            # {'name': 'set_wcs', 'crit': False},
            # {'name': 'set_skypa', 'crit': False},
            # {'name': 'set_npixsat', 'crit': False, 'args': {'satVal': 65535}},

            {'name': 'set_oa', 'crit': False},
            {'name': 'set_datlevel', 'crit': False, 'args': {'level': 0}},
            {'name': 'set_dqa_date', 'crit': False},
            {'name': 'set_dqa_vers', 'crit': False}
        ]

        return self.run_functions(funcs)

    def set_utc(self):
        """
        Extend the set_utc in instrument to first set UTC to UT if UT in header.
        """
        if self.get_keyword('UTC', False):
            return True

        ut = self.get_keyword('UT', False)
        if ut:
            self.set_keyword('UTC', ut, 'KOA: UTC keyword set from UT keyword')
            return True

        return super().set_utc()

    def make_koaid(self):
        """
        Extend the make_koaid in instrument to set KOAID based on the OFNAME,
        since KPF uses KOAID format for OFNAME.
        """
        ofname = self.get_keyword('OFNAME', useMap=False)
        if ofname:
            koaid = f"KP.{ofname.split('KP.')[-1].rstrip('.fits')}"
            if not self._validate_koaid(koaid):
                return False

            self.set_utc()
            return koaid

        return super().make_koaid()

    @staticmethod
    def _validate_koaid(koaid):
        """
        Check that the KOAID from OFNAME,  is a valid KOAID.  Assumes KP.
        are the first three characters of koaid string.

        :param koaid: <str> koaid format KP.<YYYYMMDD>.<int*5>.<int*2>
        :return:
        """
        koaid_parts = koaid.split('.')
        if len(koaid_parts) != 4:
            return False

        try:
            datetime.strptime(koaid_parts[1], '%Y%m%d')
            int(koaid_parts[2])
            int(koaid_parts[3])
        except ValueError:
            return False

        if len(koaid_parts[2]) != 5 or len(koaid_parts[3]) != 2:
            return False

        return True

    def set_inst(self):
        """
        Change the instrument from the selected instrument to KPF
        """

        selected_inst = self.get_keyword('INSTRUME')
        if not selected_inst:
            selected_inst = 'null'

        if selected_inst != 'KPF':
            self.set_keyword('INSTRUME', 'KPF',
                             f'set_inst to KPF, selected inst: {selected_inst}')

        self.set_keyword('INSTSLCT', selected_inst, 'Selected Instrument')

        return True

    def set_semester(self):
        """
        Set the semester from the DATE-OBS keyword,  this is UT date.
        """
        date_obs = self.get_keyword('DATE-OBS')

        if not date_obs:
            self.log_error('SET_SEMESTER_FAIL')
            return False

        fmt = "%Y-%m-%d"
        dt_obj = datetime.strptime(date_obs, fmt)

        # set the semester based on the date
        semA = datetime.strptime(f'{dt_obj.year}-02-01', fmt)
        semB = datetime.strptime(f'{dt_obj.year}-08-01', fmt)

        sem = 'A' if semA <= dt_obj < semB else 'B'

        # adjust year if january
        year = dt_obj.year
        if dt_obj.month == 1:
            year -= 1

        semester = f'{year}{sem}'
        self.set_keyword('SEMESTER', semester,
                         'Calculated SEMESTER from UT DATE-OBS')

        return True

    def set_elaptime(self):
        """
        Fixes missing ELAPTIME keyword.
        """

        # return if keyword already exists
        if self.get_keyword('ELAPTIME', useMap=False) is not None:
            return True

        # get necessary keywords - also GRELAPS and RDELAPS
        elasped = self.get_keyword('ELASPED', useMap=False)
        if elasped is not None:
            log.info('set_elaptime: determining ELAPTIME from KPF ELASPED')
            elaptime = round(elasped)
        else:
            elaptime = self._beg_end_time_diff(self)

        if elaptime is None:
            log.warning('set_elaptime: Could not set ELAPTIME')
            elaptime = 'null'

        # update val
        self.set_keyword('ELAPTIME', elaptime, 'KOA: Total integration time')

        return True

    def _beg_end_time_diff(self):
        """
        Calculate the elapsed time from the beginning of the observation to the
        end of the observation.

        :return: <int> the elapsed time in seconds
        """
        # ISO Format, ie: '2022-09-16T23:47:32.513382'
        tbeg = self.get_keyword('DATE-BEG', useMap=False)
        tend = self.get_keyword('DATE-END', useMap=False)

        if tbeg and tend:
            try:
                tdiff = datetime.fromisoformat(tend) - datetime.fromisoformat(tbeg)
            except ValueError:
                return None

            log.info('set_elaptime: determining ELAPTIME from DATE-END - DATE-BEG')

            return round(tdiff)

        return None

    # TODO needs to be implemented
    def set_koaimtyp(self):
        """
        Add KOAIMTYP based on algorithm, calls get_koaimtyp for algorithm
        """
        # return if keyword already exists
        if self.get_keyword('KOAIMTYP', useMap=False):
            return True

        koaimtyp = self._get_koaimtype()
        if not koaimtyp:
            log.info('set_koaimtyp: Could not determine KOAIMTYP value')
            self.log_warn("KOAIMTYP_UDF")
            koaimtyp = 'undefined'

        self.set_keyword('KOAIMTYP', koaimtyp, 'KOA: Image type')

        return True

    def _get_koaimtype(self):

        allowed = ('object', 'bias', 'dark', 'arclamp', 'flatlamp',
                   'fpc', 'domeflat', 'twiflat', 'contbars', 'undefined')

        # if instrument not defined, return
        imtype = self.get_keyword('IMTYPE')
        if not imtype:
            return 'undefined'

        imtype = imtype.lower()
        if imtype in allowed:
            return imtype

        return 'undefined'

    def set_prog_info(self):
        """
        Extend the set_prog_info in the instrument class to set PROGNAME
        by one of the PROGNAME keywords for either Green or Blue.

        Find the Progam name from either the Green or Red Progname
        keywords.  If found,  set the PROGNAME keyword so that it is found
        in the super().set_prog_info.
        """
        progid = self.progid

        if progid:
            return self._set_prog_info()

        possible_keywords = ('PROGNAME', 'GRPROGNA', 'RDPROGNA')
        for iter, kw in enumerate(possible_keywords):
            progid = self.get_keyword(kw, useMap=False)
            if progid:
                if iter != 0:
                    self.set_keyword('PROGNAME', progid,
                                     f'PROGNAME set from {kw}')
                return self._set_prog_info()

        return self._set_prog_info()

    def _set_prog_info(self):
        ok = super().set_prog_info()
        self.set_keyword('PROGTITL', self.extra_meta['PROGTITL'],
                         'KOA: Program title set')
        return ok

    def get_dir_list(self):
        """
        Function to generate the paths to all the KPF accounts, including 
        engineering

        :return: Returns the list of paths
        """
        direct_list = []
        base_path = '/s/sdata17'
        for indx1 in range(1, 10):
            base_kpf_path = f'{base_path}0{indx1}/kpf'
            for indx2 in range(1, 10):
                full_path = f'{base_kpf_path}{indx2}'
                direct_list.append(full_path)

            # engineering path
            full_path = f'{base_kpf_path}eng'
            direct_list.append(full_path)

            # development path
            full_path = f'{base_kpf_path}dev'
            direct_list.append(full_path)

        return direct_list

    def get_prefix(self):
        """
        Set prefix to KP if this is a KPF file

        :return: <str>
        """
        instr = self.get_keyword('INSTRUME')
        if 'kpf' in instr.lower():
            prefix = 'KP'
        else:
            prefix = ''

        return prefix

    # TODO,  not sure of which extension or all?
    def set_image_stats(self):
        """
        Adds mean, median, std keywords to header
        """
        return True

    def create_jpg_from_fits(self, fits_filepath, outdir):
        """
        Basic convert fits primary data to jpg.  Instrument subclasses
        can override this function.
        """
        # get image data
        hdu = fits.open(fits_filepath, ignore_missing_end=True)
        img_data = {'red': np.array([]), 'green': np.array([]),
                    'ca_hk': np.array([])}

        ext_names = list(img_data.keys())

        # get dict with number of extensions per array
        ext_lengths = self._calc_ext_lengths(hdu, ext_names)

        # order the extension data
        ext_combination = self._create_order(hdu, ext_names, ext_lengths)

        # get data into the right places
        img_data = self._mosaic_data(img_data, ext_combination)
        basename = os.path.basename(fits_filepath).replace('.fits', '')

        # form filepaths
        for extn in ext_names:
            self._write_img(img_data, extn, basename, outdir)

    # TODO define ENG for daytime cals
    def has_target_info(self):
        ut = self.get_keyword('UT', False)
        if not ut:
            return True

        if self.is_daytime(ut):
            return False

        return True

    # ---------------------
    # JPG writing functions
    # ---------------------
    @staticmethod
    def _check_extension(hdr, ext_names):
        """
        Confirm that the extension exists in the fits file.

        :param hdr: the image header object
        :param ext_names: <list> the different data exetension names.

        :return: <bool> True if the extension name is found in the header
        """
        try:
            ext_name = hdr['EXTNAME'].lower()
            for n_ext in ext_names:
                if n_ext in ext_name:
                    return True
        except KeyError:
            return False

        return False

    def _calc_ext_lengths(self, hdu, ext_names):
        """
        Determine the number of extension per array in order to mosaic the
        different amplifiers together.

        :param hdu: The astropy.io data
        :param ext_names: <list> the different data exetension names.
        :return:
        """
        ext_lengths = {}

        for indx in range(0, len(hdu)):
            hdr = hdu[indx].header
            if not self._check_extension(hdr, ext_names):
                continue

            dataname_split = hdr['EXTNAME'].lower().split('_amp')
            data_key = dataname_split[0]
            if len(dataname_split) > 1:
                data_suffix = int(dataname_split[-1][-1])
            else:
                data_suffix = 1

            if data_key not in ext_lengths or data_suffix > ext_lengths[data_key]:
                ext_lengths[data_key] = data_suffix

        return ext_lengths

    def _create_order(self, hdu, ext_names, ext_lengths):
        """
        Organize the extensions so that the mosiac adds them by the extension
        order.

        :param hdu: The astropy.io data
        :param ext_names: <list> the different data exetension names.
        :param ext_lengths: <dict> the number of extension per image (detector).
        :return:
        """
        ext_combo = {}

        for indx in range(0, len(hdu)):
            hdr = hdu[indx].header
            if self._check_extension(hdr, ext_names):
                dataname_split = hdr['EXTNAME'].lower().split('_amp')
                data_key = dataname_split[0]

                if len(dataname_split) > 1:
                    data_suffix = int(dataname_split[-1][-1]) - 1
                else:
                    data_suffix = 0

                if data_key in ext_combo:
                    ext_combo[data_key][data_suffix] = hdu[indx].data
                else:
                    ext_combo[data_key] = [None] * ext_lengths[data_key]
                    ext_combo[data_key][data_suffix] = hdu[indx].data

        return ext_combo

    @staticmethod
    def _mosaic_data(img_data, ext_combo):
        """
        Mosaic the amp extensions into one image.

        :param img_data: <dict - numpy array> the image pixel data
        :param ext_combo: The extension information

        :return: <dict - numpy array> the arraigned image pixel data
        """
        for data_key, hdu_data in ext_combo.items():
            if len(hdu_data) > 2:
                if len(hdu_data) == 3:
                    hdu_data.append(np.zeros(hdu_data[-1].shape))
                img_data[data_key] = np.vstack(
                    (np.hstack((hdu_data[0], hdu_data[1])),
                     np.hstack((hdu_data[2], hdu_data[3]))))
            else:
                for ext_data in hdu_data:
                    if img_data[data_key].size == 0:
                        img_data[data_key] = ext_data
                    else:
                        img_data[data_key] = np.hstack(
                            (img_data[data_key], ext_data))

        return img_data

    def _write_img(self, img_data, extn, basename, outdir):
        """
        Write a numpy array into a JPG file.

        :param img_data: <dict [numpy float32]> - image array to convert
        :param extn: <str> the extn name used in the data dictionary
        :param basename: <str> basename of the file to save
        :param outdir: <str> the output directory to save the files to.

        :return:
        """
        if img_data[extn] is None or img_data[extn].size == 0:
            return

        warnings.filterwarnings("ignore", category=RuntimeWarning)

        idata = img_data[extn]

        jpg_filepath = f'{outdir}/{basename}_{extn.lower()}.jpg'

        norm = ImageNormalize(idata, interval=ZScaleInterval(),
                              stretch=SqrtStretch())

        shape = idata.shape

        # create jpg
        dpi = 100
        width_inches = shape[1] / dpi
        height_inches = shape[0] / dpi
        fig = plt.figure(figsize=(width_inches, height_inches), frameon=False,
                         dpi=dpi)

        plt.axis('off')
        ax = fig.add_axes([0, 0, 1, 1])
        plt.imshow(idata, origin='lower', norm=norm, cmap='gray')
        plt.savefig(jpg_filepath)
        plt.close()

        warnings.filterwarnings("default", category=RuntimeWarning)

    # beyond level 0 functions
    def copy_drp_files(self):
        self.status['service'] = 'DRP'

        return super().copy_drp_files()

    def get_drp_files_list(self, datadir, koaid, level):
        """
        Return list of files to archive for DRP specific to KPF.

        @param datadir: <str> the location of 'stage_file'
        @param koaid: <str> the koaid of the data
        @param level: <int> the data level,  >=1
        @return:

        KPF/[DATE]/L1
            KP.20221029.25049.52_L1.fits

        /[DATE]/QLP
            KP.20221029.19915.35_1D_spectrum.png
            KP.20221029.19915.35_2D_Frame_GREEN_CCD.png
            KP.20221029.19915.35_2D_Frame_RED_CCD.png
            KP.20221029.19915.35_3_science_fibres_GREEN_CCD.png
            KP.20221029.19915.35_3_science_fibres_RED_CCD.png
            KP.20221029.19915.35_Column_cut_GREEN_CCD.png
            KP.20221029.19915.35_Column_cut_RED_CCD.png
            KP.20221029.19915.35_Histogram_GREEN_CCD.png
            KP.20221029.19915.35_Histogram_RED_CCD.png
            KP.20221029.19915.35_order_trace_GREEN_CCD.png
            KP.20221029.19915.35_order_trace_RED_CCD.png
            KP.20221029.19915.35_simple_ccf.png
        """
        if level != 1:
            raise NotImplementedError(f"Level {level} is not implemented!")

        # datadir = /koadata/KPF/stage/20221029//s/sdata1701/kpfdev/L0/KP.20221029.19915.35.fits
        # want : /kpfdata/KPF_DRP/20221029/L1/
        root_path = datadir.split('/s/')[0].replace('stage/', '').replace('koadata', 'kpfdata').replace('KPF/', 'KPF_DRP/')

        # quicklook images
        qlp_file_suffix = '.png'
        qlp_dir = f'{root_path}/QLP/fig/'
        qlp_filelist = glob.glob(f'{qlp_dir}/{koaid}*{qlp_file_suffix}')

        # lev1
        lev1_suffix = '.fits'
        lev1_dir = f'{root_path}/L1'
        lev1_filelist = glob.glob(f'{lev1_dir}/{koaid}*{lev1_suffix}')

        return lev1_filelist + qlp_filelist









