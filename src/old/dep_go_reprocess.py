'''
This is just a quick-and-dirty wrapper script to dep_go.py to run a range of dates specific to reprocessing old data.  
See input array below for usage.
'''
import sys
import os
import datetime as dt
import subprocess


# usage
if len(sys.argv) <= 6:
    print ("USAGE: dep_go_reprocess.py instr startDate endDate tpx procStart procStop useHdrProg searchDirBase metaDirBase")
    sys.exit(0)


# Go to directory of source
baseCodeDir = sys.argv[0].replace('dep_go_reprocess.py', '')
if (baseCodeDir != ""): os.chdir(baseCodeDir)


# get inputs
instr           = sys.argv[1]
startDateStr    = sys.argv[2]
endDateStr      = sys.argv[3]
tpx             = sys.argv[4]
procStart       = sys.argv[5]
procStop        = sys.argv[6]
useHdrProg      = sys.argv[7]  #"assist" or "force"
searchDirBase   = sys.argv[8]
metaDirBase     = sys.argv[9]
moveDataScript  = sys.argv[10] if len(sys.argv) > 10 else None


# loop dates and call 
startDate = dt.datetime.strptime(startDateStr, '%Y-%m-%d')
endDate   = dt.datetime.strptime(endDateStr,   '%Y-%m-%d')
curDate   = dt.datetime.strptime(startDateStr, '%Y-%m-%d')
while curDate <= endDate:

    curDateStr = curDate.strftime('%Y-%m-%d')
    searchDir = searchDirBase + '/' + curDateStr.replace('-', '')
    searchDir = searchDir.replace('//', '/')

    metaDir = metaDirBase + '/' + curDateStr.replace('-', '')
    metaDir = metaDir.replace('//', '/')

    year, month, day = curDateStr.split('-')

    print ('----------------------------------------------------')

    if not os.path.isdir(searchDir):
        print ("NOTICE: Skipping date: " + curDateStr + ". Could not find dir for: " + searchDir)    

    else:
        # cmd = "python3 dep_go.py " + instr + ' ' + curDateStr + ' ' + tpx + ' obtain tar --modtimeOverride 1 --reprocess 1 --searchDir ' + searchDir
        params = ['/usr/local/anaconda3-5.0.0.1/bin/python', 'dep_go.py', instr, curDateStr, tpx, procStart, procStop, 
                    '--modtimeOverride', '1', 
                    '--reprocess', '1', 
                    '--useHdrProg', useHdrProg, 
                    '--searchDir', searchDir,
                    '--metaCompareDir', metaDir,
                    '--emailReport', '1']
        print ('pyDEP COMMAND: ', ' '.join(params))
        subprocess.call(params)

        if moveDataScript:
            params = ['/usr/local/anaconda3-5.0.0.1/bin/python',
                        moveDataScript,
                        instr,
                        year,
                        f'--month={month}',
                        f'--day={day}',
                        '--excludeStagedFits',
                        '--confirm']
            print ('MOVE DATA COMMAND: ', ' '.join(params))
            subprocess.call(params)

        print ("DONE with date: " + curDateStr)

    curDate += dt.timedelta(days=1)


print ('----------------------------------------------------')
print ("ALL DONE")

