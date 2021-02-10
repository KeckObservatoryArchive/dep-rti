import os
import sys
import importlib 
import glob
from astropy.io import fits
import numpy as np

#import from parent dir
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parentdir)
import instrument
import metadata


def test():
    '''
    Takes a list of test FITS files and processes them all and compares them to gold standard output.
    NOTE: This test assumes your dev environment is setup in dev mode, using a dev database, and
    processing output to test ROOTDIR directores.
    '''

    #cd to script dir so relative paths work
    scriptpath = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptpath)

    #Define array of fits files to test
    #NOTE: Files must not be in proprietary period (18 months) (<2018-Jun)
    tests = [
        {'instr':'kcwi',  'rawfile':'test/input_files/kb190509_00001.fits', 'koaid':'KB.20190509.07554.57'},
        {'instr':'nires', 'rawfile':'test/input_files/s190615_0020.fits',   'koaid':'NR.20190615.12571.36'},
    ]

    for test in tests:

        instr = test['instr'].lower()

        #create instr object
        module = importlib.import_module(f'instr_{instr}')
        instr_class = getattr(module, instr.capitalize())
        instr_obj = instr_class(instr, test['rawfile'], reprocess=True, transfer=False)

        #process and make sure returns ok
        ok = instr_obj.process()
        assert ok

        #compare files both ways
        outfiles1 = get_filelist(f"test/output_files/{test['koaid']}*", basename=True)
        outfiles2 = get_filelist(f"{instr_obj.dirs['lev0']}/{test['koaid']}*", basename=True)
        for f1 in outfiles1:
            assert f1 in outfiles2, f"Temp file '{f1}' not found in temp output"
        for f2 in outfiles2:
            assert f2 in outfiles1, f"Gold file '{f2}' not found in gold output"

        #compare metadata
        files = [
            f"test/output_files/{test['koaid']}.metadata.table",
            f"{instr_obj.dirs['lev0']}/{test['koaid']}.metadata.table"            
        ]
        res = metadata.compare_meta_files(files, skipColCompareWarn=False)
        assert res, 'Metadata compare failed'
        assert len(res[0]['warnings']) == 0, 'Metadata compare differences exist'

        #compare fits headers (with skips array)
        skips = ['COMMENT', 'DQA_DATE', 'DQA_VERS']
        fits1 = fits.open(f"test/output_files/{test['koaid']}.fits")
        fits2 = fits.open(f"{instr_obj.dirs['lev0']}/{test['koaid']}.fits")
        for key, val in fits1[0].header.items():
            if key in skips: continue
            assert key in fits2[0].header, f"Gold header key '{key}' not found in temp header"
            assert val == fits2[0].header[key], f"Gold header key '{key}' val {val} != {fits2[0].header[key]}"

        #compare fits data (headers will be differnt b/c of timestamp keywords)
        assert np.array_equal(fits1[0].data, fits2[0].data), f"Fits data not the same."


def get_filelist(pattern, basename=False):
    files = []
    for filepath in glob.glob(pattern):
        if basename: filepath = os.path.basename(filepath)
        files.append(filepath)
    return files


if __name__ == "__main__":
    test()