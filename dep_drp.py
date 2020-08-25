import os
import shutil
import tarfile
import gzip
import hashlib
import subprocess



def dep_drp(instrObj, drpLevel, tpx):
    """
    This function will call the instrument DRP function if defined in config for instrument.
    """

    instr  = instrObj.instr
    utDate = instrObj.utDate
    log    = instrObj.log


    #check config for DRP directive
    drpCommand = instrObj.config[instr]['DRP'] if 'DRP' in instrObj.config[instr] else None
    if not drpCommand:
        log.info('dep_drp.py: No DRP directive found in config.')
        return

    #run it
    #todo: catch error?
    #todo: leave it up to instrument class to decide whether to run+wait?  Or use a DRP_WAIT config?
    instrObj.run_drp()
