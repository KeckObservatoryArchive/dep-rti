import datetime as dt
import os
import hashlib
import json
import glob
import re
import yaml


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

