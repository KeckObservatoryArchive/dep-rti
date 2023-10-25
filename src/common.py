import datetime as dt
import logging
import os
from sys import stdout
import hashlib
import glob
import re
import yaml
from DDOILoggerClient import DDOILogger as dl


def make_file_md5(infile, outfile):
    with open(outfile, 'w') as fp:
        md5 = hashlib.md5(open(infile, 'rb').read()).hexdigest()
        fp.write(md5 + '  ' + os.path.basename(infile) + '\n')


def make_dir_md5_table(readDir, endswith, outfile, fileList=None, regex=None):
    '''
    Create md5sum file for all files matching endswith pattern in readDir.
    Multiple files will be put into one file in table format.
    '''
    #ensure path ends in slash since we rely on that later here
    if not readDir.endswith('/'): readDir += '/'

    #get file list either direct or using 'endswith' search
    files = []
    if fileList:
        files = fileList
    else:        
        for dirpath, dirnames, filenames in os.walk(readDir):
            for f in filenames:
                if not dirpath.endswith('/'): dirpath += '/'
                match = False
                if f.endswith(".log"): continue
                if endswith and f.endswith(endswith): match = True
                elif regex and re.search(regex, f): match = True
                if match:
                    files.append(dirpath + f)
        files.sort()
        
    #create md5sum for each file and write out to single file in table format
    with open(outfile, 'w') as fp:
        for file in files:
            md5 = hashlib.md5(open(file, 'rb').read()).hexdigest()
            bName = file.replace(readDir, '')
            fp.write(md5 + '  ' + bName + '\n')


def removeFilesByWildcard(wildcardPath):
    for file in glob.glob(wildcardPath):
        os.remove(file)



def get_progid_assign(assigns, utc):
    '''
    Get fits progid assignment by time based on config option string that 
    must be formatted with comma-separated split times like the follwoing examples:
     "U205"
     "U205,10:21:00,C251"
     "U205,10:21:00,C251,13:45:56,N123"
    '''
    parts = assigns.split(',')
    assert len(parts) % 2 == 1, "ERROR: Incorrect use of ASSIGN_PROGNAME"
    if len(parts) == 1: return parts[0]

    fitsTime = dt.datetime.strptime(utc, '%H:%M:%S.%f')
    for i in range(1, len(parts), 2):
        progid = parts[i-1]
        t = parts[i]
        splitTime = dt.datetime.strptime(t, '%H:%M:%S')
        if fitsTime <= splitTime:
            return progid
    return parts[-1]


def convert_ra_dec_to_sexigesimal(coord, value):
    ''' Convert a float value to sexigecimal'''

    if not isinstance(value, (float,int)):
        return value

    # Convert from degrees to hours
    if coord != 'DEC':
        value = value / 15.0

    # Check sign for negative declinations
    sign = ''
    if value < 0:
        sign = '-'
        value = value * -1

    # Convert to HH:MM:SS.SS or DD:MM:SS.SS
    hour_deg = int(value)
    value = (value - hour_deg) * 60
    minutes = int(value)
    value = (value - minutes) * 60
    seconds = round(value, 2)
    hour_deg = str(hour_deg).zfill(2)
    minutes  = str(minutes).zfill(2)
    seconds  = str(seconds).zfill(2)
    return f"{sign}{hour_deg}:{minutes}:{seconds}"


def convert_ra_dec_to_degrees(coord, value):
    ''' Convert a float value to sexigecimal'''

    if not isinstance(value, (str)):
        return value

    # Split by :
    split = value.split(':')
    if len(split) != 3:
        return value
    split[0] = int(split[0])
    split[1] = int(split[1])
    split[2] = float(split[2])

    # Handle the sign
    sign = 1
    if split[0] < 0:
        sign = -1
        split[0] = split[0] * sign

    # Combine to a double
    newValue = split[0] + (split[1]/60.0) + (split[2]/3600.0)

    # Convert from hours to degrees
    if coord != 'DEC':
        newValue = newValue * 15.0

    # Check sign
    newValue = newValue * sign

    return newValue


def create_logger(name='koa.dep', logFile=None, configLoc='./config.live.ini', **kwargs):
    """creates a logger with the following handlers: 
    StreamHandler, ZMQHandler and the optional FileHandler.

    Args:
        name (str, optional): Name of logger. Use dot notation 'koa.dep' Defaults to 'koa.dep'.
        logFile (str, optional): name of log file for FileHandler. Defaults to None.
        configLoc (str, optional): Location of config file. Defaults to './config.live.ini'.

    Returns:
        _type_: _description_
    """

    # Create logger object
    logger = logging.getLogger(name)
    lvl = kwargs.get('logLevel', logging.INFO)
    logger.setLevel(lvl)

    #Remove all handlers
    #NOTE: This is important if processing multiple files with archive.py since
    #we reuse global log object and do some renaming of log file (see change_logger())
    logger.handlers = []

    # Create a file handler
    if logFile:
        handle = logging.FileHandler(logFile)
        lvl = kwargs.get('fileLogLevel', logging.INFO)
        handle.setLevel(lvl)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handle.setFormatter(formatter)
        logger.addHandler(handle)
        print(f'Logging to {logFile}')

    #add stdout to output so we don't need both log and print statements(>= warning only)
    sh = logging.StreamHandler(stdout)

    lvl = kwargs.get('stdoutLogLevel', logging.WARNING)
    sh.setLevel(lvl)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # add additonal log keys that we want to include in the log schema
    kwargs = { **kwargs, 'subsystem': name, 'author': __file__} 

    # load config file
    with open(configLoc) as f: 
        config = yaml.safe_load(f)
    zmq_log_handler = dl.ZMQHandler(url=config['LOGGER']['URL'], config=config, **kwargs )
    logger.addHandler(zmq_log_handler)
    
    #init message and return
    logger.info(f'logger created for {name} at {logFile}')
    return logger 