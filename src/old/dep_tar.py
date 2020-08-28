import os
import shutil
import tarfile
import gzip
import hashlib
from common import *
from datetime import datetime as dt


def dep_tar(instrObj, tpx):
    """
    This function will tar the ancillary directory, gzip that
    tarball and remove the original contents of the directory.
    """

    #define vars to use throughout
    instr  = instrObj.instr
    utDate = instrObj.utDate
    log    = instrObj.log
    dirs   = instrObj.dirs
    utDateDir = instrObj.utDateDir


    log.info('dep_tar.py started.')


    #gzip the fits files
    log.info(f'dep_tar.py gzipping fits files in {dirs["lev0"]}')
    gzip_dir_fits(dirs['lev0'])
    log.info(f'dep_tar.py gzipping fits files in {dirs["lev1"]}')
    gzip_dir_fits(dirs['lev1'])


    #tar /anc/ if exists
    if not os.path.isdir(dirs['anc']):
        log.info('dep_tar: not /anc/ dir found.  Nothing to tar.')
    else:
        log.info(f'dep_tar: tar and zipping {dirs["anc"]}.')

        # Tarball name
        tarFileName = 'anc' + instrObj.utDateDir + '.tar'

        # Go to anc directory
        myCwd = os.getcwd()
        os.chdir(dirs['anc'])

        # Create tarball
        log.info('dep_tar.py creating {}'.format(tarFileName))
        with tarfile.open(tarFileName, 'w:gz') as tar:
            tar.add('./')

        # gzip the tarball
        log.info('dep_tar.py gzipping {}'.format(tarFileName))
        gzipTarFile = tarFileName + '.gz'
        with open(tarFileName, 'rb') as fIn:
            with gzip.open(gzipTarFile, 'wb') as fOut:
                shutil.copyfileobj(fIn, fOut)

        # Remove the original tar file
        os.remove(tarFileName)

        # Create md5sum of the tarball
        md5sumFile = gzipTarFile.replace('tar.gz', 'md5sum')
        log.info('dep_tar.py creating {}'.format(md5sumFile))
        md5 = hashlib.md5(open(gzipTarFile, 'rb').read()).hexdigest()
        with open(md5sumFile, 'w') as f:
            md5 = ''.join((md5, '  ', gzipTarFile))
            f.write(md5)

        #remove anc dirs
        ancDirs = ['nightly', 'udf']
        for d in ancDirs:
            delDir = dirs['anc'] + '/' + d
            if not os.path.isdir(delDir): continue
            log.info('dep_tar.py removing {}'.format(delDir))
            shutil.rmtree(delDir)

        # go back to original directory
        os.chdir(myCwd)


    # update koatpx as archive ready
    if tpx:
        log.info('dep_dqa.py: updating tpx DB records')
        utcTimestamp = dt.utcnow().strftime("%Y%m%d %H:%M")
        update_dep_status(instr, utDate, 'arch_stat', 'DONE', log)
        update_dep_status(instr, utDate, 'arch_time', utcTimestamp, log)       
        update_dep_status(instr, utDate, 'size', get_directory_size(dirs['output']), log)


    log.info('dep_tar.py complete.')



def gzip_dir_fits(dirPath):

    for dirpath, dirnames, filenames in os.walk(dirPath):
        for f in filenames:
            if f.endswith('.fits'):
                in_path = os.path.join(dirpath, f)
                out_path = in_path + '.gz'
                with open(in_path, 'rb') as fIn:
                    with gzip.open(out_path, 'wb', compresslevel=5) as fOut:
                        shutil.copyfileobj(fIn, fOut)
                        os.remove(in_path)
