"""
Assigns programs to FITS files.  

Uses the list of the night's programs (from dep_obtain) and the list of files (from create_prog) 
to assign a program (PROGID as well as PROGINST, PROGPI, PROGTITL) to each FITS file being processed by DQA.
Output is 'newproginfo.txt' with one line per FITS containing: 
    <file> <outdir> <proginst> <progid> <progpi> <progtitl>

This has traditionally been complicated to figure out.  In an attempt to streamline and simplify,
here is the new algorithym:

- If FITS header contains PROGNAME or PROGID, use that. BREAK.  (This is done in create_prog.py)
- If it is not a split night, then assign all files to that program.
- For split nights:
 -- For each unique (non-engineering) outdir, count how many science files are within each program time range.
    If any of those program counts are a majority (> 90%), then assign outdir files to that program.
 -- Also, check if any FITS outdirs match naming scheme and assign to program based on that (_A, _B, etc) 
    if need be.  If not needed, warn if this doesn't agree with sci file method.
 -- If we still don't have an outdir assignment for a file, use the FITS UTC time to assign to overlapping program.
 -- Else, ??? (call old DEP progInfo.php?) (split)
 -- NOTE: The sci files method looks at all dirs independently, including sub dirs and TOO dirs.  It does not compare
    their counts.

Q: Do we need a min file count check on sci files method?
Q: What to do when data shows up but not scheduled?
Q: Warn of data found in non confirming dirs?

"""

import os
import json
import urllib.request as url
import time
import create_log as cl
from common import *
from dep_obtain import get_obtain_data
from datetime import datetime, timedelta
import re
from astropy.io import fits


class ProgSplit:

    def __init__(self, ut_date, instr, stage_dir, log=None):
        """
        Initialization function for the ProgSplit class

        @type ut_date: string
        @param ut_date: Date of observaton in UT timezone
        @type instr: string
        @param instr: Instrument that is being observed
        @type stage_dir: string
        @param stage_dir: directory we are moving processed files to
        """

        #save inputs
        self.utdate = ut_date
        self.instrument = instr
        self.stageDir = stage_dir
        log = log

        #consts        
        self.instrList = {  'DEIMOS'    :2, 
                            'ESI'       :2, 
                            'HIRES'     :1, 
                            'KCWI'      :2, 
                            'LRIS'      :1, 
                            'MOSFIRE'   :1, 
                            'NIRC2'     :2, 
                            'NIRSPEC'   :2, 
                            'OSIRIS'    :1, 
                            'NIRES'     :2}
        self.engineering = {'kcwieng'       :'outdir', 
                            'kcwirun'       :'outdir', 
                            'hireseng'      :'outdir',
                            'nspeceng'      :'outdir', 
                            'nirc2eng'      :'outdir', 
                            #'engineering'   :'observer',
                            'dmoseng'       :'outdir', 
                            'lriseng'       :'outdir', 
                            'esieng'        :'outdir',
                            'keck ipdm'     :'observer', 
                            'nirspec'       :'observer',
                            'nireseng'      :'outdir', 
                            'osrseng'       :'outdir',
                            'osiriseng'     :'outdir',                            
                            'moseng'        :'outdir'
                            }
        self.too = {'_ToO_':'outdir'}
        self.api = 'https://www.keck.hawaii.edu/software/db_api/'

        #var init
        self.fileList = []
        self.numFiles = 0
        self.sciTotal = 0
        self.outdirs = {}
        self.programs = []
        self.suntimes = None
        self.semester = self.get_semester()

        #log
        self.rootdir = self.stageDir.split('/stage')[0]
        if not log: log = cl.create_log(self.rootdir, instr, ut_date)

    def get_semester(self):
        """
        This method determines the semester of the observation
        using the date in UTC
        """

        #todo: move this to common (and change so that it subtracts a day and then does simple semester calc)

        # Split the date into its components
        yr, mo, dy = self.utdate.split('-')

        # convert the strings to ints
        iyr = int(yr)
        imo = int(mo)
        idy = int(dy)
        sem = 'A'

        # Determine which semester the data are from
        # Anything between August and Feb is semester B
        if imo > 8 or imo < 2:
            sem = 'B'
        # Aug 1 UT is still part of Jul 31 HST
        elif imo == 8 and idy > 1:
            sem = 'B'
        # Feb 1 UTC is part of semester B
        elif imo == 2 and idy == 1:
            sem = 'B'
        # Jan through Feb 1 are a part of the previous year's semester
        if imo == 1 or (imo ==2 and idy == 1):
            iyr -= 1
        # return the resulting semester
        return ''.join((str(iyr), sem))

    def check_stage_dir(self):
        """
        This method checks whether or not the stage dir exists
        """
        if not os.path.isdir(self.stageDir):
            raise Exception("progInfo: stage directory doesn't exist!")

    def check_instrument(self):
        """
        This method checks if the instrument is one of the existing
        instruments
        """
        if self.instrument not in self.instrList:
            raise Exception("progInfo - instrument name not valid: " + self.instrument)

    def read_file_list(self):
        """
        This method reads the list of files from the file list
        """

        #match up createprog.txt lines to column names
        colsToSave = ['file','utdate','utc','outdir','observer','frameno',
                      'imagetyp','progid','progpi','proginst','progtitl','oa']

        #check input file exists
        fname = self.stageDir +  '/createprog.txt'
        if not os.path.isfile(fname):
            raise Exception('This file does not exist!!!')
            return

        #loop thru all lines, creating one row record for each set of columns
        with open(fname, 'r') as flist:
            num = 0
            row = {}

            for line in flist:

                # Assign column to line
                row[colsToSave[num]] = line.strip()

                #check if we have reached last column to save
                #(if not already assigned valid progid then see if we can assign to ENG or ToO)
                num += 1
                if num == len(colsToSave):

                    # Check to see if it is an engineering night (Key = instrument, value = outdir/obs)
                    for key, value in self.engineering.items():
                        if key in row[value].lower() or row['progid'] == 'ENG' or row['progid'].startswith('E'):
                            if not is_progid_valid(row['progid']) or row['progid'] == 'ENG': 
                                row['proginst'] = 'KECK'
                                row['progpi']   = self.instrument.lower() + 'eng'
                                row['progtitl'] = self.instrument.upper() + ' Engineering'
                                row['progid'] = 'ENG'

                    # Check to see if it is a ToO observation (key=split, value=outdir)
                    for key, value in self.too.items():
                        if key in row[value]:
                            garbage, progid = row[value].split('_ToO_')
                            if '/' in progid:
                                progid, tmp = progid.split('/') # case of /scam and /spec
                            semid = self.semester+'_'+progid
                            row['proginst'] = get_prog_inst(semid, 'NONE', log, isToO=True)
                            row['progpi']   = get_prog_pi(semid, 'NONE', log)
                            row['progtitl'] = get_prog_title(semid, 'NONE', log)
                            row['progid']   = progid
                            #todo: should default title be "ToO Program"

                    #add row to list
                    self.fileList.append(row)

                    # Reset for the next file
                    num = 0
                    del row
                    row = {}

# ---------------- END READ FILE LIST--------------------------------------------------------

    def assign_to_pi(self, progIdx):
        """
        This method assigns program info to any file that does not already have assignment

        @type progIdx: int
        @param progIdx: index of program to assign files to
        """

        prog = self.programs[progIdx]
        log.info('getProgInfo: assign_to_pi: data: ' + str(prog))

        for i, tmpFile in enumerate(self.fileList):

            self.assign_single_to_pi(i, progIdx)

#--------------------------- END ASSIGN TO PI---------------------------------------------

    def assign_single_to_pi(self, filenum, num):
        """
        Assign a single file to a program

        @type filenum: int
        @param filenum: the index of the file to assign a PI
        @type num: int
        @param num: the index of the program to use to assign PI
        """

        # If file already has PI, skip
        # NOTE: value is blank from create_prog if engineering
        if (self.fileList[filenum]['progpi'] != 'PROGPI' and self.fileList[filenum]['progpi'] != ''):
            return

        #todo: Not sure why this is needed? legacy issue?
        if len(self.programs) == 1 and num >= 1:
            num = 0

        #get prog
        prog = self.programs[num]
        log.info('assigning ' + os.path.basename(self.fileList[filenum]['file']) + ' to progIndex: ' + str(num) + '('+prog['ProjCode']+').')

        #update col values to those in program
        self.fileList[filenum]['proginst'] = prog['Institution']
        self.fileList[filenum]['progpi']   = prog['Principal']
        self.fileList[filenum]['progid']   = prog['ProjCode']

        #assign title
        if self.fileList[filenum]['progid'] == 'ENG':
            self.fileList[filenum]['progtitl'] = self.instrument +' Engineering'
        else:
            semid = self.semester+'_'+prog['ProjCode']
            self.fileList[filenum]['progtitl'] = get_prog_title(semid, 'NONE', log)

#---------------------------- END ASSIGN SINGLE TO PI-------------------------------------------

    def assign_single_by_time(self, filenum):
        ok = False

        file = self.fileList[filenum]
        fileTime = datetime.strptime(file['utdate'] + ' ' + file['utc'], '%Y-%m-%d %H:%M:%S.%f')

        #look for program that file time falls within
        #NOTE: We actually are now just looking that the time is less than the end time of the program.
        #This means that if we actually get to the last resort of assigning by time, a time before 
        #the start of the night will just go to the first program.
        for idx in range(len(self.programs)):
            prog = self.programs[idx]
            if not prog['StartTime'] or not prog['EndTime']:
                continue
            progStartTime = datetime.strptime(self.utdate +  ' ' + prog['StartTime'],'%Y-%m-%d %H:%M')
            progEndTime   = datetime.strptime(self.utdate +  ' ' + prog['EndTime'],'%Y-%m-%d %H:%M')
            if fileTime <= progEndTime or idx == len(self.programs)-1:
                log.warning('getProgInfo: Assigning ' + os.path.basename(file['file']) + ' by time ' + file['utdate'] + ' ' + file['utc'] + ' to ' + prog['ProjCode'])
                self.assign_single_to_pi(filenum, idx)
                ok = True
                break

        return ok

#---------------------------- END ASSIGN SINGLE BY TIME -------------------------------------------


    def assign_single_by_outdir_name(self, filenum):

        #TODO: NOTE : not using this for now since it is not really reliable. Change to only use if there are multiple outdirs matching this format.
        ok = False

        #get outdir
        file = self.fileList[filenum]
        outdir = file['outdir']
        if not outdir or len(outdir) == 0: return False


        # Assign them to program indexes based on naming convention
        # (assume yyyyMMMdd, yyyyMMMdd_B...)
        assign = -1
        if   re.search('/\d{4}\D{3}\d{2}_A', outdir): assign = 0
        elif re.search('/\d{4}\D{3}\d{2}_B', outdir): assign = 1
        elif re.search('/\d{4}\D{3}\d{2}_C', outdir): assign = 2
        elif re.search('/\d{4}\D{3}\d{2}_D', outdir): assign = 3
        elif re.search('/\d{4}\D{3}\d{2}_E', outdir): assign = 4
        elif re.search('/\d{4}\D{3}\d{2}'  , outdir): assign = 0

        if assign >= len(self.programs) : 
            log.warning('getProgInfo: Program assignment index ' + str(assign) + ' > number of programs.')
            assign = -1

        if assign < 0:
            log.info('getProgInfo: Could not map ' + outdir + " to a program by dir naming convention.")
            return False
        else:
            projcode = self.programs[assign]['ProjCode']
            log.info('getProgInfo: Mapping (by name) outdir ' + outdir + " to progIndex: " + str(assign) + ' ('+projcode+').')
            self.assign_single_to_pi(filenum, assign)
            return True


    def assign_single_by_observer(self, filenum):
        '''
        Looks for matching observer names in header keyword and program listing.
        If good enough match and only one match to a program then it assigns it.
        '''

        ok = False

        #get array of names (first filter out garbage chars)
        file = self.fileList[filenum]
        observers = self.get_observer_array(file['observer'])
        # print ('assign_single_by_observer: file observer array: ', observers)
        if len(observers) == 0: return False

        #look for program with any matching names both directions
        matchIdx = -1
        for idx in range(len(self.programs)):
            prog = self.programs[idx]
            progObsvStr = prog['Observer'] + ',' + prog['Principal'] 
            progObservers = self.get_observer_array(progObsvStr)
            #print ('-- progObservers: ', progObservers)
            if len(progObservers) == 0: continue

            diff1 = len(set(observers) - set(progObservers))
            perc1 = diff1 / len(observers)
            ok1 = True if perc1 < 1.0 else False

            diff2 = len(set(progObservers) - set(observers))
            perc2 = diff2 / len(progObservers) 
            ok2 = True if perc2 < 1.0 else False
            #print ('-- result: ', diff1, perc1, ok1, diff2, perc2, ok2, matchIdx)

            #If observers match good enough then assign, but check multi program matching not ok
            if ok1 and ok2:
                if matchIdx >= 0:
                    matchIdx = -1
                    break
                else:
                    matchIdx = idx

        if matchIdx >= 0:
            log.info('getProgInfo: Assigning ' + os.path.basename(file['file']) + ' by observer match.')
            self.assign_single_to_pi(filenum, matchIdx)
            ok = True

        return ok

    def get_observer_array(self, obsvStr):
        '''
        Returns a consistent array of observer names from a string of names. Examples include:
            "Ellis Konidaris Newman Schenker Belli"
            "Ellis et al.""
            "unknown"
            "Ellis, Konidaris, Belli, Newman, & Schenkar"
        '''

        #replace whitespace, periods, slash and dash with comma
        obsvStr = re.sub("\s+", ",", obsvStr.strip())
        obsvStr = re.sub("\.+", ",", obsvStr.strip())
        obsvStr = re.sub("\/+", ",", obsvStr.strip())
        obsvStr = re.sub("\-+", ",", obsvStr.strip())

        #get rid of other unwanted things
        obsvStr = re.sub("\(.+?\)", "", obsvStr.strip())
        search  = ["_", "/", "&", ".", "and"]
        for s in search:
            obsvStr = obsvStr.replace(s, '')

        #replace multi commas with comma
        obsvStr = re.sub(",+" , ",", obsvStr.strip())

        #just keep names of length > 1
        #NOTE: We used to have cutoff at len 2, but i think this is unneccessary now.
        observers = obsvStr.split(',' )
        final = []
        for name in observers:
            name = name.strip().lower()
            if len(name) <= 1: continue
            if name in final: continue
            final.append(name)
        return final


#---------------------------- END ASSIGN SINGLE BY OBSERVER -------------------------------------------

    def get_programs(self):

        """
        This method obtains the data from the dep_obtain output file
        """

        #use obtain func
        obFile = self.stageDir + '/dep_obtain' + self.instrument + '.txt'
        self.programs = get_obtain_data(obFile)

        #if only one program and it is blank, this is actually no programs (legacy)
        if len(self.programs) == 1 and self.programs[0]['ProjCode'] == 'NONE':
            self.programs = []

#---------------------------------- END GET SCHEDULE VALUE------------------------------------

    def get_sun_times(self):
        '''
        Gets the sunrise and sunset times and calculates the two halves of the night
        NOTE: We only bother with this because for legacy data, we did not have the start/end
        times of the programs so we try to see which has the most data in either half.
        This doesn't work in the rare case that there is a same instrument 3-way or more 
        split because we couldn't predict if it was a 1/3,1/3,1/3 or a 1/2,1/4,1/4 etc.
        '''

        url = self.api + 'metrics.php?date=' + self.utdate
        self.suntimes = get_api_data(url, getOne=True)
        if not self.suntimes:
            log.error('getProgInfo: Could not get sun times via API call: ', url)
            return

#------------------------------END GET SUN TIMES------------------------------------------------

    def get_outdirs(self, programs, splitTime=None):
        '''
        Creates data array for each unique outdir which we will need if we are trying to figure out split nights.
        '''

        #make timing array for easy time comparison
        splitTimes = {}
        isMissingTimes = False
        for key, prog in enumerate(programs):

            #if split program does not have start/end (pre-keckOperations DB), then split time by sun times
            if not prog['StartTime'] or not prog['EndTime']:
                isMissingTimes = True
                sunset   = self.suntimes['sunset']
                midpoint = splitTime if splitTime else self.suntimes['midpoint']
                sunrise  = self.suntimes['sunrise']
                prog['StartTime'] = sunset   if key == 0 else midpoint
                prog['EndTime']   = midpoint if key == 0 else sunrise
                log.info('Assigning start/end times for {} to suntimes {} - {}'.format(prog['ProjCode'], prog['StartTime'], prog['EndTime']))

            t1 = datetime.strptime(prog['StartTime'], '%H:%M')
            t2 = datetime.strptime(prog['EndTime']  , '%H:%M')

            splitTimes[key] = [t1, t2]
        log.info('get_outdirs: Split times: ' + str(splitTimes))


        #throw an error if there are 3-way or more split and we don't have Start/End times
        if len(programs) > 2 and isMissingTimes:
            log.error('get_outdirs: Three or more split programs but no Start/End time info found! Program assignment may be incorrect.  Check manually.')


        #Get list of unique outdirs from file list and keep count of where the science files are
        self.outdirs = {}
        for file in self.fileList:
            fdir = self.fix_outdir(file['outdir'])
            eng = 0
            for engname, name in self.engineering.items():
                if engname in fdir: eng = 1

            #skip certain dirs/files
            if eng or fdir == '0' or 'fcs' in fdir: continue

            # Add new outdirs to the outdir list and init sci file counts
            if fdir not in self.outdirs:
                data = {'assign': -1, 'sciCounts': {}, 'sciTotal': 0}
                for i in range(len(splitTimes)):
                    data['sciCounts'][i] = 0
                self.outdirs[fdir] = data 

            #if image type is object, increment count for which program time range it falls within
            if file['imagetyp'] == 'object':
                thistime = datetime.strptime(file['utc'], '%H:%M:%S.%f')
                for i in range(len(splitTimes)):
                    if splitTimes[i][0] <= thistime and splitTimes[i][1] > thistime:
                        self.outdirs[fdir]['sciCounts'][i] += 1
                        self.outdirs[fdir]['sciTotal']     += 1
                        self.sciTotal                      += 1
                        break


#--------------------------------END GET OUTDIR-----------------------------


    def assign_outdirs_to_programs(self):

        log.info('getProgInfo: starting assign_outdirs_to_programs()')

        # Try different methods
        self.assign_outdirs_by_sci_count()


    def assign_outdirs_by_sci_count(self):

        #The idea here is: given some outdirs, count up how many science files each outdir has in each 
        #program time range. If an outdir has a high % of one program time range like > 80%, 
        # then that outdir must belong to that program.  
        #NOTE: Sometimes programs take data during other program times.

        #first check that we have multiple outdirs 
        if len(self.outdirs) <= 1:
            log.warning("getProgInfo: This is a split night but we do not have multiple outdirs.")

        log.info('getProgInfo: ' + str(len(self.outdirs)) + ' OUTDIRs found')
        for outdir, data in self.outdirs.items():
            log.info('outdir sci counts for : ' + outdir)
            for i, count in data['sciCounts'].items():
                perc = count / data['sciTotal'] if data['sciTotal'] > 0 else 0
                log.info('--- prog' + str(i) + ': ' + str(count) + ' ('+str(round(perc*100,0))+'%)')
                if (perc > 0.85 and count > 10) or (perc > 0.95 and count > 3): 
                    self.outdirs[outdir]['assign'] = i
                    projcode = self.programs[i]['ProjCode']
                    log.info('Mapping (by sci) outdir ' + outdir + " to progIndex: " + str(i) + ' ('+projcode+').')

            #no assignment?
            if self.outdirs[outdir]['assign'] < 0:
                log.warning("Could not map outdir by sci counts for: " + outdir)


    def split_multi(self):

        # Loop thru all files and if we find an outdir match, assign to program
        for idx, file in enumerate(self.fileList):
            fileOutdir = self.fix_outdir(file['outdir'])
            if fileOutdir in self.outdirs:
                progIndex = self.outdirs[fileOutdir]['assign']
                if progIndex >= 0: 
                    self.assign_single_to_pi(idx, progIndex)
                else:
                    ok = False 
                    if not ok: ok = self.assign_single_by_observer(idx)
                    #if not ok: ok = self.assign_single_by_outdir_name(idx)
                    if not ok: ok = self.assign_single_by_time(idx)
            else:
                log.error("getProgInfo: Could not find outdir match for: " + fileOutdir)

            #final check to see if assigned
            if self.fileList[idx]['progpi'] in ('PROGPI', '', 'NONE'):
                log.error("getProgInfo: Could not assign program for file: " + os.path.basename(self.fileList[idx]['file']))

#---------------------END SPLIT MULTI ----------------------------------------

    def fix_outdir(self, outdir):
        """
        Function to remove unwanted subdirectories from outdir

        @type outdir: string
        @param outdir: path to be have subdirs removed
        """
        if '/fcs' in outdir:
            outdir = outdir.replace('/fcs', '')
        rep = ['/s/', '//', '/scam/', '/spec/', '/scam', '/spec']
        for subdir in rep:
            outdir = outdir.replace(subdir, '/')
        return outdir

#-----------------------------END FIX SUBDIR--------------------------------

    def sort_by_time(self, progs):
        """
        Simple Bubble Sort algorithm to reorder multiple nights by StartTime
        """
        cont = True
        while(cont):
            cont = False
            for i in range(len(progs)-1):
                if not progs[i]['StartTime']:
                    cont = False
                    break
                elif (time.strptime(progs[i]['StartTime'],'%H:%M') > time.strptime(progs[i+1]['StartTime'],'%H:%M')):
                    temp = progs[i]
                    progs[i] = progs[i+1]
                    progs[i+1] = temp
                    del temp
                    cont = True

#----------------------------------END SORT BY TIME----------------------------------

    def get_header_val(self, header, key, default=None):
        val = header.get(key)
        if val != None and not isinstance(val, fits.Undefined): 
            return val
        else:
            return default


    def use_header_prog_vals(self, use):

        for idx, file in enumerate(self.fileList):

            #read in fits file header only
            header = fits.getheader(file['file'], 0)

            #See if any of the PROG* keywords don't match old header
            #If we could not determine (ie NONE), use old header value and warn
            #Else, use new value and error with VERIFY reminder.
            keywords = ['progid', 'proginst', 'progpi', 'progtitl']
            for kw in keywords:
                val = ''
                if kw == 'progtitl':
                    val1 = self.get_header_val(header, 'PROGTL1', '')
                    val2 = self.get_header_val(header, 'PROGTL2', '')
                    val3 = self.get_header_val(header, 'PROGTL3', '')
                    if val1: val +=       val1.strip()
                    if val2: val += ' ' + val2.strip()
                    if val3: val += ' ' + val3.strip()
                else:
                    val = self.get_header_val(header, kw.upper(), '')

                #determine whether to use new or old val (only throw warn/error for PROGID)
                if use == 'assist':
                    if val and val != file[kw]:
                        if file[kw] and file[kw] != 'NONE' and 'PROG' not in file[kw]:
                            if kw == 'progid':
                                log.error("getProgInfo: " + kw.upper() + " value mismatch. VERIFY new value for: " + os.path.basename(file['file']))
                        else:
                            self.fileList[idx][kw] = val
                            if kw == 'progid':
                                log.info("getProgInfo: Could not determine " + kw.upper() + " value. Assigning from old header for: " + os.path.basename(file['file']))
                elif use == 'force':
                    if val and val != file[kw]:
                        self.fileList[idx][kw] = val
                        if kw == 'progid':
                            log.warning("getProgInfo: Force assigning " + kw.upper() + " from old header for: " + os.path.basename(file['file']))

#--------------------------------------------------------------------

    def logStats(self):

        counts = {} 
        for prog in self.programs:
            pc = prog['ProjCode']
            counts[pc] = 0

        for progfile in self.fileList:
            progid = progfile['progid']
            if progid not in counts:
                counts[progid] = 0
            counts[progid] += 1

        for key, count in counts.items():
            if key == 'PROGID': key = 'NONE'
            log.info(f"getProgInfo: PROGID COUNT: {key}: {count}")


#--------------------------------------------------------------------



def getProgInfo(utdate, instrument, stageDir, useHdrProg=False, splitTime=None, log=None, test=False):

    if test: 
        rootdir = stageDir.split('/stage')[0]
        log = cl.create_log(rootdir, instrument, utdate+'_TEST')

    #input var compat
    utdate = utdate.replace('/','-')
    instrument = instrument.upper()

    #gather info
    progSplit = ProgSplit(utdate, instrument, stageDir, log)
    progSplit.check_stage_dir()
    progSplit.check_instrument()
    progSplit.read_file_list()

    #get list of programs and determine if instrument split night
    progSplit.get_programs()
    numSplits = len(progSplit.programs) if progSplit.programs else 0

    #No split
    if numSplits == 1: 
        progSplit.log.info('getProgInfo: ' + utdate + ' is not a split night')
        progSplit.log.info('getProgInfo: Assigning to ' + progSplit.instrument + ' PI (' + progSplit.programs[0]['ProjCode'] + ')')
        progSplit.assign_to_pi(0)

    # Split night
    elif numSplits > 1: 
        progSplit.sort_by_time(progSplit.programs)
        progSplit.log.info('getProgInfo: ' + utdate + ' is a split night with ' + str(len(progSplit.programs)) + ' programs: ' + str(progSplit.programs))
        progSplit.get_sun_times()
        progSplit.get_outdirs(progSplit.programs, splitTime)
        progSplit.assign_outdirs_to_programs()
        progSplit.split_multi()

    #no proj codes
    # TODO: only throw error if there was some science files (ie this could be engineering)
    else:
        if progSplit.sciTotal > 0:
            progSplit.log.warning(f"No {instrument} programs scheduled this night but {self.sciTotal} science files taken.")


    #special reprocessing check to look in header for PROG* info if all else fails
    if useHdrProg: 
        progSplit.use_header_prog_vals(useHdrProg)


    #write out result
    fname = stageDir + '/newproginfo.txt'
    if test: fname += '.TEST'
    with open(fname, 'w') as ofile:
        for progfile in progSplit.fileList:
            line =         progfile['file']
            line += "\t" + progfile['outdir']
            line += "\t" + progfile['proginst']
            line += "\t" + progfile['progid']
            line += "\t" + progfile['progpi']
            line += "\t" + progfile['progtitl']
            line += "\n"
            ofile.writelines(line)

    #log stats
    progSplit.logStats()


    #return data written for convenience
    progSplit.log.info('getProgInfo: finished, {} created'.format(fname))
    return progSplit.fileList
