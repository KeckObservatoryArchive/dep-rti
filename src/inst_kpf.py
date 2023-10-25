
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
import matplotlib.pyplot as plt
from PIL import Image
from astropy.visualization import ZScaleInterval, AsinhStretch, SinhStretch
from astropy.visualization.mpl_normalize import ImageNormalize
from mpl_toolkits.axes_grid2 import ImageGrid

import logging
koa_dep_logger logging.getLogger('koa.dep')


class KPF(instrument.Instrument):
    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)