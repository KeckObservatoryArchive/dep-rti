
"""
This is the class to handle all the KPF specific attributes
"""

import instrument
from common import *

import matplotlib as mpl
mpl.use('Agg')

import logging
main_logger = logging.getLogger(DEFAULT_LOGGER_NAME)


class KPF(instrument.Instrument):
    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid)