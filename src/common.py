import datetime as dt
import os
import hashlib
from urllib.request import urlopen
import json
from send_email import send_email
import glob
import re
import yaml
import db_conn



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




def do_fatal_error(msg, instr=None, utDate=None, failStage=None, log=None):

    #read config vars
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    
    #form subject
    subject = 'DEP ERROR: ['
    if (instr)     : subject += instr     + ' '
    if (utDate)    : subject += utDate    + ' '
    if (failStage) : subject += failStage + ' '
    subject += ']'

    #always print
    print (subject + ' ' + msg)

    #if log then log
    if log: log.error(subject + ' ' + msg)

    #if admin email and not dev then email
    if (adminEmail != ''):
        send_email(adminEmail, adminEmail, subject, msg)



def update_dep_status(instr, utDate, column, value, log=''):
    """
    Sends command to update KOA data

    @param instrObj: the instrument object
    @param column: column to update in koa.koatpx
    @type column: string
    @param value: value to update column to
    @type value: string
    """
    db = db_conn.db_conn('config.live.ini', configKey='DATABASE')

    # If entry not in database, create it
    query = f'select count(*) as num from koatpx where instr="{instr}" and utdate="{utDate}"'
    check = db.query('koa', query, getOne=True)
    if check is False:
        if log: log.error(f'update_dep_status failed for: {instr}, {utDate}, {column}, {value}')
        return False
    if int(check['num']) == 0:
        query = f'insert into koatpx set instr="{instr}", utdate="{utDate}"'
        if log: log.info(query)
        check = db.query('koa', query)
        if check is False or int(check) == 0:
            if log: log.error(f'update_dep_status failed for: {instr}, {utDate}, {column}, {value}')
            return False

    # Now update it
    query = f'update koatpx set {column}="{value}" where instr="{instr}" and utdate="{utDate}"'
    if log: log.info(query)
    check = db.query('koa', query)
    if check is False:
        if log: log.error(f'update_dep_status failed for: {instr}, {utDate}, {column}, {value}')
        return False

    return True


def get_directory_size(dir):
    """
    Returns the directory size in MB

    @param dir: directory to determine size for
    @type dir: string
    """

    #directory doesn't exist
    if not os.path.isdir(dir):
        return 0

    #walk through and sum up size
    total = 0
    for dirpath, dirnames, filenames in os.walk(dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return str(total/1000000.0)




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

