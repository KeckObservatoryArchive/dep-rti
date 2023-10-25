import pytest
import filecmp
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
                 'KCWI': {'KB', 'KF', 'KR'}, 
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
serverName = os.uname()[1]
if 'koaserver' in serverName:
    outDir = '/tmp/dep_test'
    pytestPath = '/usr/local/anaconda3-5.0.0.1/bin/pytest'
    koadataPath = os.path.join('/koadata', 'koadata_test')
elif 'koarti' in serverName:
    outDir = '/tmp/dep_test'
    pytestPath = 'pytest'
    koadataPath = os.path.join('/koadata', 'koadata_test')
else:
    outDir = './tmp'
    pytestPath = 'pytest'
    koadataPath = os.path.join(os.pardir, os.pardir, os.pardir, 'koadata_test')
fitsFilePath = os.path.join(koadataPath, 'test', '**', '20210208', 'lev0')
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
    logger = logging.getLogger(logFile)
    logger.info(startMsg)
    for inst in INST_MAPPING.keys():
        keywordsDefFile = glob(os.path.join(keywordTablePath, f'KOA_{inst}_Keyword_Table.txt'))[0]
        metaOutFile = os.path.join(os.getcwd(), outDir, f'dep_{inst}.metadata.table') # must end in metadata.table
        instFitsFilePath = fitsFilePath.replace('**', inst)
        extraData = create_extra_data()
        metadata.make_metadata(keywordsDefFile, metaOutFile, instFitsFilePath, extraData, log, dev=dev, filePath=None)

@pytest.mark.metadata
def test_input_tables_exist():
    assert os.path.exists(keywordTablePath), 'check KeywordsTable dir is set correctly'
    for inst in INST_MAPPING.keys():
        keywordsDefFile = glob(os.path.join(keywordTablePath, f'KOA_{inst}_Keyword_Table.txt'))[0]
        assert os.path.exists(keywordsDefFile), f'check that {inst} table exists'

@pytest.mark.metadata
def test_fits_files_exist():
    for inst in INST_MAPPING:
        fitsFiles = glob(os.path.join(fitsFilePath, '*.fits'))
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

@pytest.mark.metadata
def test_compare_metadata_files():
    '''compares metadatafiles with standard found in koadata_test'''
    outFiles = glob(os.path.join(outDir, f'*.metadata.table'))
    filesMismatch = []
    for idx in range(len(outFiles)):
        f1 = outFiles[idx]
        f2 = glob(os.path.join(koadataPath, 'dep_rti_output_std', os.path.basename(f1)))[0]
        assert os.path.basename(f1) == os.path.basename(f2), 'file names must match {0} != {1}'.format(f1, f2)
        if not filecmp.cmp(f1, f2, False):
            filesMismatch.append(f1)
    assert len(filesMismatch) == 0, 'mismatching files with standard: {}'.format(filesMismatch)
@pytest.mark.metadata
def test_compare_md5sum_files():
    '''compares checksum files with standard found in koadata_test'''
    metaOutFiles = glob(os.path.join(outDir, f'*.metadata.md5sum'))
    filesMismatch = []
    for idx in range(len(metaOutFiles)):
        f1 = metaOutFiles[idx]
        f2 = glob(os.path.join(koadataPath, 'dep_rti_output_std', os.path.basename(f1)))[0]
        assert os.path.basename(f1) == os.path.basename(f2), 'file names must match {0} != {1}'.format(f1, f2)
        if not filecmp.cmp(f1, f2, False):
            filesMismatch.append(f1)
    assert len(filesMismatch) == 0, 'mismatching files with standard: {}'.format(filesMismatch)

if __name__=='__main__':
    if not os.path.exists(outDir): 
        os.mkdir(outDir)
    logging.basicConfig(filename=logFile, level=logging.DEBUG)
    create_tables_and_checksum_files()
    # run pytests
    testCmd = pytestPath + ' -m metadata'
    os.system(testCmd)
    # cleanup
    if not dev:
        rmtree(outDir)
