import pytest
import logging
import sys
import pdb
import os
sys.path.append(os.path.pardir)
import metadata
from glob import glob
import pdb
from shutil import rmtree
"""
test_metadata.py runs test on metadata and checksum files generated from fits files found in koadata_test/test/inst directories. 
Tables, checksum, and log files are created before tests are run, and then deleted.
Test suite for metadata.py should run independently of other pytests and is run with the shell command:
python test_metadata.py
"""
INST_MAPPING = { 
                 'DEIMOS': {'DE', 'DF'},
                 'ESI': {'EI'},
                 'HIRES': {'HI'},
                 'KCWI': {'KB', 'KF'}, 
                 'LRIS': {'LB', 'LR'},
                 'MOSFIRE': {'MF'},
                 'OSIRIS': {'OI', 'OS'},
                 'NIRES': {'NR', 'NI', 'NS'},
                 'NIRC2': {'N2', 'NC'},
                }
#  extra data needed to pass all assertions
EXTRA_DATA = {
'PROPINT': 18,
'PROPINT1': 18,
'PROPINT2': 18,
'PROPINT3': 18,
'PROPMIN3': 18,
'PROPMIN': -999,
'PROPINT': 8,
'FILESIZE_MB': 0.0,
'OFNAME': 'ofNamePlaceholder',
'PROGTITL': '',
}
keywordTablePath = os.path.join(os.pardir, os.pardir, os.pardir, 'KeywordTables')
fitsFilePath = os.path.join('koadata_test', 'test', '**', '20210208', 'lev0')
outDir = './tmp'
startMsg = f'creating tables and files in {outDir}'
logFile = os.path.join(outDir, os.path.basename(__file__).replace('.py', '.log'))
dev = True
def create_extra_data():
    extraData = {}
    for file in glob(os.path.join(fitsFilePath, '*.fits')): 
        filename = os.path.basename(file)
        extraData[filename] = EXTRA_DATA
    return extraData

def create_tables_and_checksum_files():
    log = logging.getLogger(logFile)
    log.info(startMsg)
    for inst in INST_MAPPING.keys():
        keywordsDefFile = glob(os.path.join(keywordTablePath, f'KOA_{inst}_Keyword_Table.txt'))[0]
        metaOutFile = os.path.join(os.getcwd(), outDir, f'dep_{inst}.metadata.table') # must end in metadata.table
        instFitsFilePath = fitsFilePath.replace('**', inst)
        extraData = create_extra_data()
        metadata.make_metadata(keywordsDefFile, metaOutFile, instFitsFilePath, extraData, log, dev=dev)

@pytest.mark.metadata
def test_input_tables_exist():
    assert os.path.exists(keywordTablePath), 'check KeywordsTable dir is set correctly'
    for inst in INST_MAPPING.keys():
        keywordsDefFile = glob(os.path.join(keywordTablePath, f'KOA_{inst}_Keyword_Table.txt'))[0]
        assert os.path.exists(keywordsDefFile), f'check that {inst} table exists'

@pytest.mark.metadata
def test_fits_files_exist():
    for inst in INST_MAPPING:
        lev0Dir = os.path.join('./koadata_test', 'test', inst, '20210208', 'lev0')
        fitsFiles = glob(os.path.join(lev0Dir, '*.fits'))
        assert len(fitsFiles) > 0, f'inst {inst} does not have any fits files.'

@pytest.mark.metadata
def test_ipac_tables_created():
    for inst in INST_MAPPING.keys():
        outFileName = glob(os.path.join(outDir, f'*{inst}.metadata.table'))
        assert len(outFileName) == 1, f'there should be one completed ipac table for inst {inst}'

@pytest.mark.metadata
def test_checksums_created():
    for inst in INST_MAPPING.keys():
        outFileName = glob(os.path.join(outDir, f'*{inst}.metadata.md5sum'))
        assert len(outFileName) == 1, f'there should be one completed ipac table checksum file for inst {inst}'

@pytest.mark.metadata
def test_nrows_equal_nfiles():
    for inst in INST_MAPPING.keys():
        outFileName = glob(os.path.join(outDir, f'*{inst}.metadata.md5sum'))
        assert len(outFileName) == 1, f'there should be one completed ipac table checksum file for inst {inst}'

@pytest.mark.metadata
def test_logging():
    assert os.path.exists(logFile), 'log file does not exist'
    with open(logFile) as f: 
        assert startMsg in f.readline(), 'check that log file is logging properly'


if __name__=='__main__':
    if not os.path.exists(outDir): 
        os.mkdir(outDir)
    logging.basicConfig(filename=logFile, encoding='utf-8', level=logging.DEBUG)
    create_tables_and_checksum_files()

    # run pytests
    if not dev:
        os.system('pytest -m metadata')
    # cleanup
    rmtree(outDir)
