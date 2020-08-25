from astropy.io import fits
from urllib.request import urlopen
from dep_obtain import get_obtain_data
from common import *


def create_prog(instrObj):
    '''
    Creates a temporary staging data file "createprog.txt' listing all 
    program information for each file.  This file is input to getProgInfo.py.
    The following data are written out one value per line per fits file:

        file
        utdate
        utc
        outdir
        observer
        frameno
        imagetyp
        progid
        progpi
        proginst
        progtitl
        oa
        <repeats here>

    @type instrObj: instrument
    @param instr: The instrument object
    '''


    #short vars
    instr      = instrObj.instr
    utDate     = instrObj.utDate
    stageDir   = instrObj.dirs['stage']
    log        = instrObj.log


    #info
    if log: log.info('create_prog: Getting FITS file information')


    # Get OA from dep_obtain file
    obFile = stageDir + '/dep_obtain' + instr + '.txt'
    obData = get_obtain_data(obFile)
    oa = ''
    if len(obData) >= 1: oa = obData[0]['OA']


    # Get all files
    fileList = []
    locateFile = stageDir + '/dep_locate' + instr + '.txt'
    with open(locateFile, 'r') as loc:
        for item in loc:
            fileList.append(item.strip())


    # loop through files and add data to createprog.txt
    outfile = stageDir + '/createprog.txt'
    with open(outfile, 'w') as ofile:
        for filename in fileList:

            #skip blank lines
            if filename.strip() == '': continue

            #skip OSIRIS files that end in 'x'
            if instr == 'OSIRIS':
                if filename[-1] == 'x':
                    log.info(filename + ': file ends with x')
                    continue

            #load fits into instrObj
            #todo: Move all keyword fixes as standard steps done upfront?
            instrObj.set_fits_file(filename)

            # Temp fix for bad file times (NIRSPEC legacy)
            instrObj.fix_datetime(filename)

            #get image type
            instrObj.set_koaimtyp()
            imagetyp = instrObj.get_keyword('KOAIMTYP')

            #get date-obs
            instrObj.set_dateObs()
            dateObs = instrObj.get_keyword('DATE-OBS')

            #get utc
            instrObj.set_utc()
            utc = instrObj.get_keyword('UTC')

            #get observer
            observer = instrObj.get_keyword('OBSERVER')
            if observer == None: observer = 'None'
            observer = observer.strip()

            #get fileno
            fileno = instrObj.get_fileno()

            #get outdir
            outdir = instrObj.get_outdir()

            #lop off everything before /sdata
            # fileparts = filename.split('/sdata')
            # if len(fileparts) > 1: newFile = '/sdata' + fileparts[-1]
            # else                 : newFile = filename
            #TODO: NOTE: removing this string split since is causing problems with new code and I don't think it is necessary
            newFile = filename

            # Get the semester
            instrObj.set_semester()
            sem = instrObj.get_keyword('SEMESTER')
            sem = sem.strip()

            #write out vars to file, one line each var
            newFile = newFile.replace('//','/')
            ofile.write(newFile+'\n')
            ofile.write(dateObs+'\n')
            ofile.write(utc+'\n')
            ofile.write(outdir+'\n')
            ofile.write(observer+'\n')
            ofile.write(str(fileno)+'\n')
            ofile.write(imagetyp+'\n')

            #if PROGNAME exists (either assigned from command line or in PROGNAME), use that to populate the PROG* values
            #NOTE: PROGNAME can be in format with or without semester
            if instrObj.config['MISC']['ASSIGN_PROGNAME']:
                progname = get_progid_assign(instrObj.config['MISC']['ASSIGN_PROGNAME'], utc)
                if log: log.info(f"Force assigning {os.path.basename(newFile)} to PROGID '{progname}'")
            else:
                progname = instrObj.get_keyword('PROGNAME')
                if progname != None: progname = progname.replace('ToO_', '')            

            #valid progname?
            isProgValid = is_progid_valid(progname)
            if progname and not isProgValid:
                if log: log.warn('create_prog: Invalid PROGNAME: ' + str(progname))

            #try to assign PROG* keywords from progname
            progid   = 'PROGID'
            progpi   = 'PROGPI'
            proginst = 'PROGINST'
            progtitl = 'PROGTITL'
            if isProgValid:
                progname = progname.strip().upper()
                if progname == 'ENG':
                    progid = 'ENG'
                else:
                    if '_' in progname: sem, progname = progname.split('_')
                    semid = sem + '_' + progname
                    progid   = progname
                    progpi   = get_prog_pi   (semid, 'PROGPI'  , log)
                    proginst = get_prog_inst (semid, 'PROGINST', log)
                    progtitl = get_prog_title(semid, 'PROGTITL', log)

            ofile.write(progid + '\n')
            ofile.write(progpi   + '\n')
            ofile.write(proginst + '\n')
            ofile.write(progtitl + '\n')

            #write OA last
            ofile.write(oa + '\n')

    if log: log.info('create_prog: finished, {} created'.format(outfile))


