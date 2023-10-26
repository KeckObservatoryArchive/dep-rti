
"""
This is the class to handle all the KPF specific attributes
"""

import instrument
import datetime as dt
from common import *
import numpy as np
from astropy.io import fits
from scipy import ndimage

import matplotlib as mpl
mpl.use('Agg')

import logging
main_logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class KPF(instrument.Instrument):
    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)