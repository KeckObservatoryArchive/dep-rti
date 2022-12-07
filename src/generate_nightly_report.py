import os
import argparse
from glob import glob
    
import db_conn
from datetime import datetime, timedelta
from collections import Counter
import pdb


from getpass import getuser
from os.path import dirname, isfile
import requests
from socket import gethostname
from astropy.io import fits as fits
import yaml

def get_config():
    configPath = os.path.realpath(os.path.dirname(__file__) )
    configPath = os.path.join(configPath, 'config.live.ini')
    url = None
    if isfile(configPath):
        with open(configPath) as f:
            config = yaml.safe_load(f)
    return config

def send_to_slack(body, instrument, debug=False):

    config = get_config()
    url = config.get('API', {}).get('SLACKAPP', None)
    if url != None and not debug:
        data = {}
        data['text'] = body

        msg = requests.post(url, json=data)
        if msg.status_code != 200:
            print('Error sending message to Slack')
    else:
        print(body)

def get_database():
    configPath = os.path.realpath(os.path.dirname(__file__) )
    configPath = os.path.join(configPath, 'config.live.ini')
    return db_conn.db_conn(configPath, configKey='DATABASE', persist=True)
    

def del_db(db):
    if db: db.close()


def get_daily_table(db, date, endDate, instr, level=0):

    dateStr = datetime.strftime(date ,TIME_FORMAT)
    endDateStr = datetime.strftime(endDate,TIME_FORMAT)
    query = (f"select * from koa_status where level={level} and "
                f" instrument='{instr}' ")
    query += f" and date(utdatetime)>=date('{dateStr}')"
    query += f" and date(utdatetime)<date('{endDateStr}')"
    query += " order by id asc"
    rows = db.query('koa', query)
    return rows
    

def get_database_metrics(rows):
    ofname = [x.get('ofname') for x in rows]
    reviewed = [x.get('reviewed') for x in rows]
    incomplete = [x for x in rows if x['status'].upper()=='INCOMPLETE']
    error = [x for x in rows if x['status'].upper()=='ERROR']
    invalid = [x for x in rows if x['status'].upper()=='INVALID']
    statusCodes = [x.get('status_code') for x in rows]
    notReviewedStatusCodes = [ statusCodes[idx] for idx in \
                              range(len(statusCodes)) if reviewed[idx]==0 ]
    uniqueStatusCodes = dict(Counter(notReviewedStatusCodes).items())
    if '' in uniqueStatusCodes.keys():
        del uniqueStatusCodes['']


    dirs = [ "/".join(x.split('/')[0:-1]) for x in ofname]
    dirs = [*set(dirs)]

    basenames = [ x.split('/')[-1][0:5] for x in ofname]
    basenames = [*set(basenames)]

    numIncomplete = len(incomplete) 
    numError = len(error)
    numInvalid = len(invalid)
    return (dirs, 
            reviewed,
            uniqueStatusCodes, 
            numIncomplete, 
            numError, 
            numInvalid, 
            basenames)
    

def count_dir_files(dirs, startDate, endDate):
    files = []

    startHIDate = startDate - timedelta(hours=10)
    endHIDate  = endDate   - timedelta(hours=10)
    startTimestamp = datetime.timestamp(startHIDate)
    endTimestamp  = datetime.timestamp(endHIDate)

    def within_range(filePath):
        ts = os.path.getctime(filePath)
        return ts >= startTimestamp and ts < endTimestamp 

    for dir in dirs:
        query = f'{dir}/*.fits'
        newFiles = glob(query)
        # Filter out timestamps outside of the time range
        newFiles = [ x for x in newFiles if within_range(x) ]

        files = [*files, *newFiles]

    files = [*set(files)] # remove duplicates
    return {
            'numFiles': len(files),
            'files': files
           }

def count_nirspec_dir_files(dirs, startDate, endDate):
    nspecFiles = []
    nscamFiles = []

    startTimestamp = datetime.timestamp(startDate)
    endTimestamp  = datetime.timestamp(endDate)

    def within_range(filePath):
        ts = os.path.getctime(filePath)
        return ts >= startTimestamp and ts < endTimestamp 

    for dir in dirs:
        query = f'{dir}/*.fits'
        newFiles = glob(query)
        # Filter out timestamps outside of the time range
        newFiles = [ x for x in newFiles if within_range(x) ]

        service = dir.split('/')[-1]
        if 'scam' in service: 
            nscamFiles = [*nscamFiles, *newFiles]

        if 'spec' in service:
            nspecFiles = [*nspecFiles, *newFiles]


    files = [*nspecFiles, *nscamFiles]
    # remove duplcates
    nFiles = len(files)

    return {
            'numFiles': nFiles, 
            'numNspec': len(nspecFiles), 
            'numNscam': len(nscamFiles),
            'files': files 
           }

def get_missing_files(filesDict, rows):
    
    ofnames = [x['ofname'].replace('//', '/') for x in rows]
    missingFiles = []
    for file in filesDict['files']:
        if not file.replace('//','/') in ofnames:
            missingFiles.append(file)
    return missingFiles

def filter_out_basenames(missingFiles, basenames):
    filteredFiles = [fileName for fileName in missingFiles\
                              for basename in basenames\
                              if basename in fileName ]
    return filteredFiles

def make_report(instrument, 
                date, 
                dirs, 
                reviewed,
                uniqueStatusCodes, 
                numIncomplete, 
                numError, 
                numInvalid,
                basenames,
                numDirFiles, 
                scheduled,
                numLev0DBFiles,
                numLev1DBFiles,
                numLev2DBFiles,
                missingFiles):
    strDirs = ",\n\t".join(dirs)

    dateStr = datetime.strftime(date, '%Y-%m-%d')
    isScheduled = 'Yes' if scheduled else 'No'

    numFilesTotal = numDirFiles.get('numFiles')
    numDBFiles = numLev0DBFiles + numLev1DBFiles + numLev2DBFiles
    if numDBFiles==0:
        percentComplete=0
    else:
        percentComplete = round( 100 * (numDBFiles - numIncomplete - numError - \
                                        numInvalid)/ numDBFiles )

    report = f"""\n
    Instrument:\t {instrument}
    UT date:\t {dateStr}
    Scheduled?:\t {isScheduled}
    *Number of FITS in OUTDIRs:*\t {numFilesTotal}
    *Number of ingestions:*\t {numDBFiles}
    *% complete:*\t {percentComplete}
    Data directories:\n\t {strDirs}
    """

    status_code_report = "Status_Code Errors/Warnings:\n"

    for key, val in uniqueStatusCodes.items():
        status_code_report += f'{key}:\t {val}\n'

    if uniqueStatusCodes and not reviewed:
        report += status_code_report

    if instrument.upper() == 'NIRSPEC':
        numNspec = numDirFiles.get('numNspec', False)
        numNscam= numDirFiles.get('numNscam', False)
        if numNspec:
            report += f"""Number of nspec FITS in OUTDIRS:\t {numNspec}\n"""
        if numNscam:
            report += f"""    Number of nscam FITS in OUTDIRS:\t {numNscam}\n"""

    missingFiles.sort()
    if len(missingFiles) > 50:
        missingFiles = missingFiles[0:49]
        strMissingFiles = ",\n\t".join(missingFiles[0:49]).replace(' ', '')
        strMissingFiles += "\n\t and more..."
    else:
        strMissingFiles = ",\n\t".join(missingFiles).replace(' ', '')
    warnMsg = f"""
    =================================
    Error: non ingested files found: {len(missingFiles)}
    =================================
    {strMissingFiles}
    """

    incompleteMsg = """

    =================================
    Incomplete: Found incomplete ingests: {numIncomplete}
    =================================
    Number of incomplete ingests:\t {numIncomplete}
    """

    invalidMsg = f"""
    =================================
    Invalid: Found invalid files: {numInvalid} 
    =================================
    Number of invalid ingests:\t {numInvalid}
    """

    errMsg = f"""
    =================================
    Error: Found errored files: {numError}
    =================================
    Number of errored ingests:\t {numError}
    """

    level = 'INFO :relieved:'

    if len(missingFiles) > 0: 
        report += warnMsg
        level = ':fire::fire::fire:CRITICAL:fire::fire::fire:'
    if numIncomplete > 0:
        report += incompleteMsg
        level = ':warning:WARNING:warning:'
    if numInvalid> 0:
        report += invalidMsg 
        level = ':warning:WARNING:warning:'
    if numError > 0:
        report += errMsg
        level = ':fire::fire::fire:CRITICAL:fire::fire::fire:'
    report = f"\nLevel: {level}" + report

    return report 

def get_associated_fcs_files(filesDict, rows, missingFiles):
    files = filesDict.get('files', [])
    whitelist = []
    for row in rows:
        filename = row.get('ofname', '')
        if not os.path.exists(filename):
            continue
        hdus = fits.open(filename)
        hdr = hdus[0].header
        fcsreffi = hdr.get('FCSREFFI')
        if not fcsreffi:
            continue
        whitelist.append(fcsreffi)
    return whitelist 

        

def generate_report(instrument, date, debug=False):
    scheduled = is_instrument_scheduled(date, instrument)
    db = get_database()
    lev0Rows = get_daily_table( db, date, endDate, instrument, level=0 )
    numLev0DBFiles = len( lev0Rows )
    lev1Rows = get_daily_table( db, date, endDate, instrument, level=1 )
    numLev1DBFiles = len( lev1Rows )
    lev2Rows = get_daily_table( db, date, endDate, instrument, level=2 )
    numLev2DBFiles = len( lev2Rows )

    rows = [*lev0Rows, *lev1Rows, *lev2Rows]
    metrics = get_database_metrics(rows)
    dirs = metrics[0]

    basenames = metrics[-1]


    filesDict = count_nirspec_dir_files(dirs, date, endDate) \
            if instrument.upper() == 'NIRSPEC' \
            else count_dir_files(dirs, date, endDate)

    missingFiles = get_missing_files(filesDict, rows)

    missingFiles = filter_out_basenames(missingFiles, basenames)

    if instrument.upper() == 'DEIMOS':
        whitelist = get_associated_fcs_files(filesDict, rows, missingFiles)
        # remove referenced fcs files from missingFile list
        missingFiles = [ x for x in missingFiles if x[2:] in whitelist ]

    missingFiles = [*set(missingFiles)]

    report = make_report(instrument,
                         date, 
                         *metrics,
                         filesDict, 
                         scheduled,
                         numLev0DBFiles,
                         numLev1DBFiles,
                         numLev2DBFiles,
                         missingFiles
                        )

    numFilesTotal = filesDict.get('numFiles')
    # ignore non scheduled and silent instruments
    if not scheduled and numFilesTotal==0: 
        print('inst not scheduled and silent. not sending to slack.')
        send_to_slack(report, instrument.upper(), True)
    else:
        send_to_slack(report, instrument.upper(), debug)

def is_instrument_scheduled(date, inst):
    config = get_config()
    telApi = config.get('API', {}).get('TELAPI')
    telSchedDate = date - timedelta(days=1) # tel sched uses HT
    dateStr = datetime.strftime(telSchedDate, '%Y-%m-%d')
    if inst.upper()=='NIRSPEC':
        inst = 'NIRSP'
    url = f"{telApi}cmd=getSchedule&date={dateStr}&instr={inst}"
    resp = requests.get(url)
    sched = resp.json() # either a list or empty dict
    if len(sched)>0:
        return True
    return False

if __name__ == '__main__':
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    parser = argparse.ArgumentParser(description='generate_nightly_report input parameters')
    parser.add_argument('--instrument', type=str, help='instrument to check that generates report')
    parser.add_argument('--date', type=str, help='date', default=None)
    parser.add_argument('--dome', type=str, help='dome is K1 or K2', default=None)
    parser.add_argument('--debug', type=bool, help='debug prints out report instead of sending it to Slack.', default=False)

    args = parser.parse_args()
    debug = args.debug

    dateStr = args.date
    if dateStr:
        date = datetime.strptime(dateStr, TIME_FORMAT)
    else:
        date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = (date + timedelta(days=1))
    instrument = args.instrument
    
    if instrument:
        generate_report(instrument, date, debug)
    else:
        dome = args.dome
        if not dome:
            hostname = os.uname()[1]
            dome = 'K1' if hostname == 'vm-koarti' else 'K2'
        config = get_config()
        instruments = config.get(dome.upper(), [])
        [generate_report(x, date, debug) for x in instruments]

