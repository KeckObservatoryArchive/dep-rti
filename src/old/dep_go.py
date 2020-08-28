from sys import argv
import argparse
from datetime import datetime
from dep import *
import configparser
from common import *
import traceback
import os

# Go to directory of source

baseCodeDir = sys.argv[0].replace('dep_go.py', '')
if (baseCodeDir != ""): os.chdir(baseCodeDir)

# Define Input parameters

parser = argparse.ArgumentParser(description='DEP input parameters')
parser.add_argument('instr'         , type=str,                             help='Instrument name')
parser.add_argument('utDate'        , type=str, nargs='?', default=None,    help='UTC Date (yyyy-mm-dd) to search for FITS files in prior 24 hours. Default is current date.')
parser.add_argument('tpx'           , type=int, nargs='?', default=0,       help='Update TPX database?  [0, 1].  Default is 0.')
parser.add_argument('procStart'     , type=str, nargs='?', default=None,    help='(OPTIONAL) Name of process to start at. ["obtain", "locate", "add", "dqa", "lev1", "tar", "koaxfr"]. Default is "obtain".')
parser.add_argument('procStop'      , type=str, nargs='?', default=None,    help='(OPTIONAL) Name of process to stop at. ["obtain", "locate", "add", "dqa", "lev1", "tar", "koaxfr"]. Default is "koaxfr".')
parser.add_argument('--searchDir'   , type=str, nargs='?', const=None,      help='(OPTIONAL) Directory to search (recursively) for FITS files.  Default search dirs are defined in instrument class files.')
parser.add_argument('--reprocess'   , type=str, nargs='?', const=None,      help='(OPTIONAL) Set to "1" to indicate reprocessing old data (skips certain locate/search checks)')
parser.add_argument('--modtimeOverride' , type=str, nargs='?', const=None,  help='(OPTIONAL) Set to "1" to ignore modtime on files during FITS locate search.')
parser.add_argument('--metaCompareDir'  , type=str, nargs='?', const=None,  help='(OPTIONAL) Directory to use for special metadata compare report for reprocessing old data.')
parser.add_argument('--useHdrProg'  , type=str, nargs='?', const=None,      help='(OPTIONAL) Set to "force" to force header val if different.  Set to "assist" to use only if indeterminate (useful for processing old data).')
parser.add_argument('--splitTime'   , type=str, nargs='?', const=None,      help='(OPTIONAL) HH:mm of suntimes midpoint for overriding split night timing.')
parser.add_argument('--emailReport' , type=str, nargs='?', default="0",       help='(OPTIONAL) Set to "1" to send email report whether or not it is a full run')
parser.add_argument('--assignProgname' , type=str, nargs='?', default='',    help='(OPTIONAL) Force assign all data to provided progname (ie U190 or 2020A_U190). Can use split time str like "U205,10:21:00,C251"')

# Get input params

args = parser.parse_args()
instr  = args.instr.upper()
utDate = args.utDate
tpx    = args.tpx
pstart = args.procStart
pstop  = args.procStop
emailReport = args.emailReport
assignProgname = args.assignProgname

# Get Config args

configArgs = []
if args.searchDir      : configArgs.append({'section':'LOCATE', 'key':'SEARCH_DIR',         'val': args.searchDir})
if args.modtimeOverride: configArgs.append({'section':'LOCATE', 'key':'MODTIME_OVERRIDE',   'val': args.modtimeOverride})
if args.reprocess      : configArgs.append({'section':'MISC',   'key':'REPROCESS',          'val': args.reprocess})
if args.metaCompareDir : configArgs.append({'section':'MISC',   'key':'META_COMPARE_DIR',   'val': args.metaCompareDir})
if args.useHdrProg     : configArgs.append({'section':'MISC',   'key':'USE_HDR_PROG',       'val': args.useHdrProg})
if args.splitTime      : configArgs.append({'section':'MISC',   'key':'SPLIT_TIME',         'val': args.splitTime})
configArgs.append({'section':'MISC',   'key':'EMAIL_REPORT',       'val': args.emailReport})
configArgs.append({'section':'MISC',   'key':'ASSIGN_PROGNAME',    'val': args.assignProgname})

# Use the current UT date if none provided
if (utDate == None): utDate = datetime.utcnow().strftime('%Y-%m-%d')

# Create and run Dep (wrap in try to catch any runtime errors and email them)

try:
    dep = Dep(instr, utDate, tpx=tpx, configArgs=configArgs)
    dep.go(pstart, pstop)
except Exception as error:
    msg = traceback.format_exc()
    do_fatal_error(msg, instr, utDate, 'dep_go')
