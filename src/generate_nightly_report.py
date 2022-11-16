import os
import argparse
from glob import glob
    
import db_conn
from datetime import datetime, timedelta
import pdb


from getpass import getuser
from os.path import dirname, isfile
import requests
from socket import gethostname
import yaml

def send_to_slack(body, instrument):

    configPath = os.path.realpath(os.path.dirname(__file__) )
    configPath = os.path.join(configPath, 'config.live.ini')
    url = None
    if isfile(configPath):
        with open(configPath) as f:
            config = yaml.safe_load(f)

        url = config.get('API', {}).get('SLACKAPP', None)
    if url != None:
        data = {}
        data['text'] = body

        msg = requests.post(url, json=data)
        if msg.status_code != 200:
            print('Error sending message to Slack')
    else:
        print('Error sending message to Slack - no URL')

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
    incomplete = [x for x in rows if x['status'].upper()=='INCOMPLETE']
    error = [x for x in rows if x['status'].upper()=='ERROR']
    invalid = [x for x in rows if x['status'].upper()=='INVALID']

    dirs = [ "/".join(x.split('/')[0:-1]) for x in ofname]
    dirs = [*set(dirs)]

    numIncomplete = len(incomplete) 
    numError = len(error)
    numInvalid = len(invalid)
    return (dirs, numIncomplete, numError, numInvalid)
    

def count_dir_files(dirs, startDate, endDate):
    files = []

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
    
    ofnames = [x['ofname'] for x in rows]
    missingFiles = []
    for file in filesDict['files']:
        if not file in ofnames:
            missingFiles.append(file)
    return missingFiles

def make_report(instrument, 
                date, 
                dirs, 
                numIncomplete, 
                numError, 
                numInvalid,
                numDirFiles, 
                numLev0DBFiles,
                numLev1DBFiles,
                numLev2DBFiles,
                missingFiles):
    strDirs = ",\n\t".join(dirs)
    report = f"""\n
    Instrument:\t {instrument}
    UT date:\t {date}
    Number of incomplete ingests:\t {numIncomplete}
    Number of errored ingests:\t {numError}
    Number of invalid ingests:\t {numInvalid}
    Data directories:\n\t {strDirs}
    """
    if instrument.upper() == 'NIRSPEC':
        numNspec = numDirFiles.get('numNspec', False)
        numNscam= numDirFiles.get('numNscam', False)
        if numNspec:
            report += f"""Number of nspec files in Directories:\t {numNspec}\n"""
        if numNscam:
            report += f"""    Number of nscam files in Directories:\t {numNscam}\n"""

    numFilesTotal = numDirFiles.get('numFiles')
    numDBFiles = numLev0DBFiles + numLev1DBFiles + numLev2DBFiles
    report += f"""
    lev0 files in Database:\t {numLev0DBFiles}
    lev1 files in Database:\t {numLev1DBFiles}
    lev2 files in Database:\t {numLev2DBFiles}
    *Total number of files in Directories:*\t {numFilesTotal}
    *Total number of files in Database:*\t {numDBFiles}
    """

    strMissingFiles = ",\n\t".join(missingFiles)
    warnMsg = f"""
    =================================
    Warning: non ingested files found 
    =================================
    {strMissingFiles}
    """

    incompleteMsg = """

    =================================
    Incomplete: Found incomplete ingests: {numIncomplete}
    =================================
    """

    invalidMsg = f"""
    =================================
    Invalid: Found invalid files: {numInvalid} 
    =================================
    """

    errMsg = f"""
    =================================
    Error: Found errored files: {numError}
    =================================
    """

    level = 'INFO'

    if len(missingFiles) > 0: 
        report += warnMsg
        level = 'WARNING'
    if numIncomplete > 0:
        report += incompleteMsg
        level = 'WARNING'
    if numInvalid> 0:
        report += invalidMsg 
        level = 'WARNING'
    if numError > 0:
        report += errMsg
        level = 'CRITICAL'
    report = f"\nLevel: {level}" + report

    return report 

def generate_report(instrument, date):
    db = get_database()
    lev0Rows = get_daily_table( db, date, endDate, instrument, level=0 )
    numLev0DBFiles = len( lev0Rows )
    lev1Rows = get_daily_table( db, date, endDate, instrument, level=1 )
    numLev1DBFiles = len( lev1Rows )
    lev2Rows = get_daily_table( db, date, endDate, instrument, level=2 )
    numLev2DBFiles = len( lev2Rows )

    rows = [*lev0Rows, *lev1Rows, *lev2Rows]
    metrics = get_database_metrics(rows)

    filesDict = count_nirspec_dir_files(metrics[0], date, endDate) \
            if instrument.upper() == 'NIRSPEC' \
            else count_dir_files(metrics[0], date, endDate)

    missingFiles = get_missing_files(filesDict, rows)

    report = make_report(instrument,
                         date, 
                         *metrics,
                         filesDict, 
                         numLev0DBFiles,
                         numLev1DBFiles,
                         numLev2DBFiles,
                         missingFiles
                        )

    send_to_slack(report, instrument.upper())

def get_instruments(date, dome):
    url = f"https://www.keck.hawaii.edu/software/db_api/telSchedule.php?cmd=getSchedule&date={datetime.strftime(date, '%Y-%m-%d')}"
    resp = requests.get(url)
    schedInstruments = [ x.get('Instrument') for x in resp.json() if x.get('Instrument', False)]
    schedInstruments = [*set(schedInstruments)]
    ignoreList = ['SSC', 'PCS', 'PCS+SSC']
    schedInstruments = [ x for x in schedInstruments if not x in ignoreList ]


    schedInstNames = []
    for schedInst in schedInstruments:
        for k, v in INSTRUMENT_MAP.items():
            if schedInst in v:
                schedInstNames.append(k)
                break

    schedInstNames = [ x for x in schedInstNames if x in DOME_INSTRUMENTS[dome] ] 
    return schedInstNames 
    
def get_instrument_list():
    url = lambda x: f"https://www.keck.hawaii.edu/software/db_api/telSchedule.php?cmd=getSchedule&date={datetime.strftime(x, '%Y-%m-%d')}"

    date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    dates = [ date - timedelta(days=x) for x in range(400) ] 
    instruments=[]
    for date in dates: 
        resp = requests.get(url(date))
        dayInst = [ x.get('Instrument') for x in resp.json() if x.get('Instrument', False) ]
        instruments = [*instruments,  *dayInst]

    return [*set(instruments)] # return unique values

INSTRUMENT_MAP = {
    'NIRC2': ['NIRC2-LGS', 'NIRC2-NGS', 'NIRC2-PY'],
    'NIRSPEC': ['NIRSPAO-NGS', 'NIRC2-PY+NIRSPAO', 'NIRSPAO+LGS', 'NIRC2-NGS+NIRSPAO', 'NIRSPEC'],
    'ESI': ['ESI'],
    'KPF': ['KPF'],
    'SSC': ['SSC'],
    'MOSFIRE': ['MOSFIRE'],
    'NIRES': ['NIRES'], 
    'LRIS': ['LRIS-ADC'],
    'OSIRIS': ['OSIRIS-LGS, OSIRIS-NGS'],
    'KCWI': ['KCWI'],
    'PCS': ['PCS', 'PCS+SSC'],
    'DEIMOS': ['DEIMOS'],
    'HIRES': ['HIRESr', 'HIRESb'],
}

DOME_INSTRUMENTS = {
    'K2': [ 'KCWI', 'DEIMOS', 'ESI', 'NIRC2', 'NIRES', 'NIRESPEC' ],
    'K1': [ 'HIRES', 'OSIRIS', 'MOSFIRE', 'KPF', 'LRIS' ]
}

if __name__ == '__main__':
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    parser = argparse.ArgumentParser(description='generate_nightly_report input parameters')
    parser.add_argument('--instrument', type=str, help='instrument to check that generates report')
    parser.add_argument('--date', type=str, help='date', default=None)
    parser.add_argument('--dome', type=str, help='date', default=None)

    args = parser.parse_args()
    dome = args.dome
    if not dome:
        hostname = os.uname()[1]
        dome = 'K1' if hostname == 'vm-koarti' else 'K2'

    dateStr = args.date
    if dateStr:
        date = datetime.strptime(dateStr, TIME_FORMAT)
    else:
        date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = (date + timedelta(days=1))
    instrument = args.instrument
    
    if instrument:
        generate_report(instrument, date)
    else:
        instruments = get_instruments(date, dome)
        [generate_report(x, date) for x in instruments]

