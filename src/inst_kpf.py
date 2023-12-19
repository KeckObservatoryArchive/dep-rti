
"""
This is the class to handle all the KPF specific attributes
"""

import instrument
from common import *

import matplotlib as mpl
mpl.use('Agg')


class KPF(instrument.Instrument):
    def __init__(self, instr, filepath, reprocess, transfer, progid, dbid=None, logger_name=DEFAULT_LOGGER_NAME):
        super().__init__(instr, filepath, reprocess, transfer, progid, dbid, logger_name)