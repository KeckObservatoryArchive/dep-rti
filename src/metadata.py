"""
  This script creates the archiving metadata file as part of the DQA process.

  Original scripts written in IDL by Jeff Mader
  Ported to python by Josh Riley

"""
import sys
from functools import wraps
import os
from astropy.io import fits
from astropy.coordinates import Angle
import astropy.units as au
from numpy import nan, isnan
import datetime
import re
import pandas as pd
import html
import pdb
import glob
import gzip
import hashlib
import json
import logging
from pathlib import Path
import traceback

log = logging.getLogger("koa_dep")


class Metadata():

    def __init__(self, keyDefFile, metaOutFile, searchdir=None, fitsfile=None, 
                  extraMeta=dict(), dev=False, keyskips=[], create_md5=False):
        """
        Creates the archiving metadata file as part of the DQA process.

        - keyDefFile (string): keywords format definition input file path
        - metaOutFile (string): metadata output file path
        - searchdir( string): directory for finding FITS files and writing output files
        - fitsfile (string): Full path to single fits file to create metadata for
        - extraMeta (dict): dictionary of any extra key val pairs not in header
        - dev (bool): Are we in dev mode (affects what warns are reported)
        - keyskips (array): Keywords to skip existence warnings.
        - create_md5 (bool): Create md5sum file of metadata table
        """
        self.keyDefFile = keyDefFile
        self.metaOutFile = metaOutFile
        self.searchdir = searchdir
        self.fitsfile = fitsfile
        self.extraMeta = extraMeta
        self.dev = dev
        self.keyskips = keyskips
        self.create_md5 = create_md5


    def make_metadata(self):
        '''
        Creates the archiving metadata file as part of the DQA process.
        Will raise an exception if an error is encountered, otherwise will return 
        an array of warnings.
        '''

        #open keywords format file and read data
        log.info('metadata.py reading keywords definition file: {}'.format(self.keyDefFile))
        keyDefs = pd.read_csv(self.keyDefFile, sep='\t')
        try:
            keyDefs = self.format_keyDefs(keyDefs)
        except Exception as err:
            raise Exception(f'Error parsing keyword defs file: {self.keyDefFile}.\n{traceback.format_exc()}')

        #check first col is KOAID
        assert keyDefs.iloc[0].keyword == "KOAID", "First column must be KOAID"

        #create initial file with header
        self.init_metadata_file(self.metaOutFile, keyDefs)

        #collect warnings
        self.warns = []

        #get all fits files
        log.info('metadata.py searching fits files in dir: {}'.format(self.searchdir))
        fitsFiles = []
        if self.searchdir:
            for path in Path(self.searchdir).rglob('*.fits'):
                fitsFiles.append(str(path))
        if self.fitsfile:
            fitsFiles.append(self.fitsfile)
        if len(fitsFiles) == 0:
            log.info(f'No fits file(s) found')

        #loop fits files and add a meta row for each
        for fitsFile in sorted(fitsFiles):
            extra = {}
            baseName = os.path.basename(fitsFile)
            if baseName in self.extraMeta:
                extra = self.extraMeta[baseName]
            self.add_fits_metadata_line(fitsFile, self.metaOutFile, keyDefs, extra)

        #md5sum option
        if self.create_md5: 
            self.create_md5_checksum_file(self.metaOutFile)

        return self.warns


    def format_keyDefs(self, keyDefs):
        '''renames and type declarations for metadata table'''
        keyDefs = keyDefs.rename(columns={'FITSKeyword': 'keyword', 'MetadataDatatype': 'metaDataType', 'NullsAllowed':'allowNull', 'MetadataWidth': 'colSize', 'MinValue': 'minValue', 'MaxValue': 'maxValue'})
        keyDefs = keyDefs.dropna(axis=0, subset=['keyword'])
        keyDefs = keyDefs[keyDefs['Source'].astype(str) != 'NExScI']
        keyDefs['colSize'] = keyDefs['colSize'].astype(int)
        #keyDefs['minValue'] = keyDefs['minValue'].astype(float)
        #keyDefs['maxValue'] = keyDefs['maxValue'].astype(float)
        return keyDefs

    def init_metadata_file(self, filename, keyDefs):
        #add header to output file
        with open(filename, 'w+') as out:

            #check col width is at least as big is the keyword name
            for index, row in keyDefs.iterrows():
                if (len(row['keyword']) > row['colSize']):
                    keyDefs.loc[index, 'colSize'] = len(row['keyword'])
            for index, row in keyDefs.iterrows():
                out.write('|' + row['keyword'].ljust(row['colSize']))
            out.write("|\n")
            for index, row in keyDefs.iterrows():
                out.write('|' + row['metaDataType'].ljust(row['colSize']))
            out.write("|\n")
            for index, row in keyDefs.iterrows():
                out.write('|' + ''.ljust(row['colSize']))
            out.write("|\n")

            for index, row in keyDefs.iterrows():
                nullStr = '' if (row['allowNull'] == "N") else "null"
                out.write('|' + nullStr.ljust(row['colSize']))
            out.write("|\n")
            out.flush()


    def create_md5_checksum_file(self, metaOutFile):
        md5OutFile = metaOutFile.replace('.table', '.md5sum')
        metaOutPath = os.path.dirname(metaOutFile)
        with open(md5OutFile, 'w') as fp:
            md5 = hashlib.md5(open(metaOutFile, 'rb').read()).hexdigest()
            bName = os.path.basename(metaOutFile)
            fp.write(md5 + '  ' + bName + '\n')
            fp.flush()


    def warn(self, code, msg):
        self.warns.append({'code':code, 'msg':msg})


    def add_fits_metadata_line(self, fitsFile, metaOutFile, keyDefs, extra):
        """
        Adds a line to metadata file for one FITS file.
        """
        log.info("Creating metadata record for: " + fitsFile)

        #get header object using astropy
        header = fits.getheader(fitsFile)
        #check keywords
        self.check_keyword_existance(header, keyDefs, extra)
        #write all keywords vals for image to a line
        with open(metaOutFile, 'a') as out:

            for index, row in keyDefs.iterrows():

                keyword   = row['keyword']
                dataType  = row['metaDataType']
                colSize   = row['colSize']
                allowNull = row['allowNull']

                #get value from header, set to null if not found
                if keyword in header: 
                    try:
                        val = header[keyword]
                    except Exception as e:
                        self.warn('MD_HEADER_KEYWORD_UNREADABLE', f'{fitsFile}: {keyword}')
                        val = 'null'
                elif keyword in extra:
                    val = extra[keyword]
                else: 
                    val = 'null'
                    if self.dev: 
                        self.warn('MD_HEADER_KEYWORD_MISSING', f'{fitsFile}: {keyword}')

                #special check for val = fits.Undefined
                if isinstance(val, fits.Undefined):
                    val = 'null'

                #special check for 'NaN' or '-Nan'
                if val in ('NaN', '-NaN'):
                    val = 'null'

                #check keyword val and format
                try:
                    val = self.check_keyword_val(keyword, val, row)
                except Exception as err:
                    msg = f'check_keyword_val ERROR. file: {fitsFile}, keyword: {keyword} val: {val}.\n{traceback.format_exc()}'
                    raise Exception(msg)

                #write out val padded to size
                out.write(' ')
                out.write(str(val).ljust(colSize))
                out.flush()
            out.write("\n")


    def check_keyword_existance(self, header, keyDefs, extra={}):

        #get simple list of keywords
        keyDefList = []
        for index, row in keyDefs.iterrows():
            keyDefList.append(row['keyword'])        

        #find all keywords in header that are not in metadata file
        skips = ['SIMPLE', 'COMMENT', 'PROGTL1', 'PROGTL2', 'PROGTL3'] + self.keyskips
        for keywordHdr in header:
            if not keywordHdr: continue  #blank keywords can exist
            if keywordHdr not in keyDefList and not is_keyword_skip(keywordHdr, skips):
                if self.dev: 
                    self.warn('MD_KEYWORD_FORMAT_UNDEFINED', f'{keywordHdr}')

        #find all keywords in metadata def file that are not in header
        skips = ['PROGTITL', 'PROPINT']
        for index, row in keyDefs.iterrows():
            keyword = row['keyword']
            if keyword not in header and keyword not in skips and row['allowNull'] == "N" and keyword not in extra:
                if self.dev: 
                    self.warn('MD_HEADER_KEYWORD_MISSING', f'{keywordHdr}')


    def check_keyword_val(self, keyword, val, fmt):
        '''
        checks keyword for correct type and proper value.
        '''
        #specific error, udf values that we should convert to "null"
        errvals = ['#### Error ###']
        if (val in errvals):
            val = 'null'

        #deal with null, blank vals
        self.check_null(val, fmt['allowNull'])
        if (val == 'null' or val == '') and (fmt['allowNull'] == 'Y'):
            return val

        #basic checks of type and length
        mtype = fmt['InputFormat'] if fmt['InputFormat'] else fmt['metaDataType']
        val = self.fix_value(val, fmt['metaDataType'], keyword)
        if fmt['ValidateFormat'].upper() == 'Y':
            if not self.check_value_type(val, mtype, keyword):
                raise Exception(f"{keyword} val '{val}' is not type {mtype}")
        val = self.check_and_set_char_length(val, fmt['colSize'], fmt['metaDataType'], fmt['keyword'])
        val = self.convert_type(val, fmt['metaDataType'])

        #check range and discrete values?
        if fmt['CheckValues'].upper() == 'Y':
            # check if val is angle in degrees
            if not pd.isnull(fmt['minValue']) and mtype == 'angle':
                ang = Angle(val, au.deg)
                minAng = Angle(fmt['minValue'], au.deg)
                maxAng = Angle(fmt['maxValue'], au.deg)
                if ang < minAng:
                    self.warn('MD_RANGE_ERROR', f'{keyword} val {ang} < minVal {minAng}')
                if ang > maxAng:
                    self.warn('MD_RANGE_ERROR', f'{keyword} val {ang} > maxVal {maxAng}')
            else:
                self.check_min_range(val, fmt['minValue'], fmt['metaDataType'], keyword)
                self.check_max_range(val, fmt['maxValue'], fmt['metaDataType'], keyword)
                self.check_discrete_values(val, fmt['DiscreteValues'], keyword)

        return val


    def check_min_range(self, val, minVal, vtype, keyword):
        if is_none(minVal): return
        if val < self.convert_type(minVal, vtype):
            self.warn('MD_RANGE_ERROR', f'{keyword} val {val} < minVal {minVal}')


    def check_max_range(self, val, maxVal, vtype, keyword):
        if is_none(maxVal): return
        if val > self.convert_type(maxVal, vtype):
            self.warn('MD_RANGE_ERROR', f'{keyword} val {val} > maxVal {maxVal}')


    def check_discrete_values(self, val, valStr, keyword):
        '''Discrete value string can be JSON or comma-separated.'''
        if is_none(valStr): return
        try:
            valSet = json.loads(valStr)
        except Exception as e:
            valSet = valStr.split(',')

        valSet = [x.strip().lower() for x in valSet]
        if not val.lower() in valSet:
            self.warn('MD_DISCRETE_VAL_ERROR', f'{keyword} val "{val}" not in {valSet}')


    def check_and_set_char_length(self, val, colSize, metaDataType, keyword):
        length = len(str(val))
        if (length > colSize):
            if (metaDataType == 'double'): 
                self.warn('MD_TRUNCATE_DOUBLE', f'char length of {length} > col size of {colSize} ({keyword}={val}).  TRUNCATING.')
                val = truncate_float(val, colSize)
            else: 
                self.warn('MD_TRUNCATE', f'char length of {length} > col size of {colSize} ({keyword}={val}).  TRUNCATING.')
                val = str(val)[:colSize]
        return val


    def check_value_type(self, val, mtype, keyword):
        '''Check that we can cast to expected type.'''
        try:
            if   mtype == 'integer':  val = int(val)
            elif mtype == 'double':   val = float(val)
            elif mtype == 'date':     val = datetime.datetime.strptime(val, '%Y-%m-%d')
            elif mtype == 'time':     val = datetime.datetime.strptime(val, '%H:%M:%S.%f')
            elif mtype == 'datetime': val = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            elif mtype == 'angle':    val = Angle(val, au.deg)
        except:
            return False
        return True


    def check_null(self, val, allowNull):
        if (val == 'null' or val == '') and (allowNull == 'N'):
            raise Exception(f'Incorrect "null" value found for non-null keyword {keyword}')            


    def convert_type(self, val, vtype):
        '''convert to either integer or float if vtype specifies'''
        try:
            if vtype=='integer':
                return int(val)
            elif vtype=='double':
                return float(val)
            else:
                return val
        except Exception as e:
            raise Exception(f'Could not convert val "{val}" to type {type}')


    def fix_value(self, val, metaDataType, keyword):
        '''Some known value conversions we must do.'''
        if (metaDataType == 'char'):
            if isinstance(val, bool):
                if   (val == True):  val = 'T'
                elif (val == False): val = 'F'
            elif isinstance(val, int) and val == 0:
                val = ''
                log.info(f'metadata check: {keyword}: found integer 0, expected {metaDataType}. KNOWN ISSUE. SETTING TO BLANK!')
        return val


def is_none(x):
    '''checks that floats and ints are None or nan'''
    nones = (None, nan)
    if not isinstance(x, (int, float)):
        return False
    return (x in nones) or (isnan(x))


def is_keyword_skip(keyword, skips):
    for pattern in skips:
        if re.search(pattern, keyword):
            return True
    return False


def truncate_float(f, n):
    s = '{}'.format(f)
    exp = ''
    if 'e' in s or 'E' in s:
        parts = re.split('e', s, flags=re.IGNORECASE)
        s = parts[0]
        exp = 'e' + parts[1]
    n -= len(exp)
    return s[:n] + exp


def compare_meta_files(filepaths, skipColCompareWarn=False):
    '''
    Takes an array of filepaths to metadata output files and compares them all to 
    the first metadata file in a smart manner.
    '''
    results = []

    #columns we always skip value check
    skips = ['DQA_DATE', 'DQA_VERS']

    #store list of columns to compare
    compareCols = []
    compareKoaids = []

    #loop, parse and store dataframes
    dfs = []
    for filepath in filepaths:
        data = load_metadata_file_as_df(filepath)
        if isinstance(data, pd.DataFrame): dfs.append(data)
        else                             : return False

    #compare all to first df in list
    baseDf = dfs[0]
    baseColList = baseDf.columns.tolist()
    for i, df in enumerate(dfs):
        if i == 0: continue

        result = {}
        result['compare'] = '==> comparing (0){} to ({}){}:'.format(baseDf.name, i, df.name)
        result['warnings'] = []

        #basic two-way column name compare
        colList = df.columns.tolist()
        for col in colList:
            if col not in baseColList:
                if col not in skips:
                    if not skipColCompareWarn: 
                        result['warnings'].append('Meta compare: MD{} col "{}" not in MD0 col list.'.format(i, col))
            else:
                if col not in compareCols: compareCols.append(col)
        for col in baseColList:
            if col not in colList:
                if col not in skips:
                    if not skipColCompareWarn: 
                        result['warnings'].append('Meta compare: MD0 col "{}" not in MD{} col list.'.format(col, i))
            else:
                if col not in compareCols: compareCols.append(col)

        #basic two-way row find using koaid value
        for index, row in df.iterrows():
            koaid = row['KOAID']
            baseRow = baseDf[baseDf['KOAID'] == koaid]
            if baseRow.empty: 
                result['warnings'].append('Meta compare: CANNOT FIND KOAID "{}" in MD0'.format(koaid))
                continue
            else:
                if koaid not in compareKoaids: compareKoaids.append(koaid)

        for index, baseRow in baseDf.iterrows():
            koaid = baseRow['KOAID']
            row = df[df['KOAID'] == koaid]
            if row.empty: 
                result['warnings'].append('Meta compare: CANNOT FIND KOAID "{}" in MD{}'.format(koaid, i))
                continue
            else:
                if koaid not in compareKoaids: compareKoaids.append(koaid)

        #for koaids we found in both, compare those rows
        for koaid in compareKoaids:
            row0 = baseDf[baseDf['KOAID'] == koaid].iloc[0]
            row1 = df[df['KOAID'] == koaid].iloc[0]
            for col in compareCols:
                if col in skips: continue

                val0 = row0[col]
                val1 = row1[col]

                if val_smart_diff(val0, val1, col):
                    result['warnings'].append('Meta compare: {}: col "{}": (0)"{}" != ({})"{}"'.format(koaid, col, val0, i, val1))

        results.append(result)

    return results

def val_smart_diff(val0, val1, col=None):

    #turn pandas null to blank 
    if pd.isnull(val0): val0 = ''
    if pd.isnull(val1): val1 = ''

    #special fix for progtitl
    if col == 'PROGTITL':
        val0 = val0.replace('  ',' ')
        val1 = val0.replace('  ',' ')

    #try to decimal format (if not then no problem)
    try:
        newval0 = "{:.1f}".format(float(val0))
        newval1 = "{:.1f}".format(float(val1))
    except:
        newval0 = val0
        newval1 = val1
    val0 = newval0
    val1 = newval1

    #diff
    isDiff = False
    val0 = str(val0).lower()
    val1 = str(val1).lower()
    if val0 != val1:

        #if different, try html escaping
        if html.escape(val0) != html.escape(val1):
            isDiff = True

    return isDiff

def load_metadata_file_as_df(filepath):

    if not os.path.isfile(filepath): return False

    with open(filepath, 'r', errors='replace') as f:

        # Read first line of header and find all column widths using '|' split
        header = f.readline().strip()
        cols = header.split('|')
        colWidths = []
        for col in cols:
            w = len(col)
            if w <= 1: continue;
            colWidths.append(w+1)

        #read fixed-width formatted metadata file using calculated col widths and remove garbage
        data = pd.read_fwf(filepath, widths=colWidths, skiprows=range(1,4))
        data.columns = data.columns.str.replace('|','')
        data.columns = data.columns.str.strip()

        data.name = os.path.basename(filepath)
        return data

    return None

def header_keyword_report(keyDefFile, fitsFile):

    #read keywords format file and fits file
    keyDefs = pd.read_csv(keyDefFile, sep='\t')
    header = fits.getheader(fitsFile, ignore_missing_end=True)

    #put header keys into set
    print ("=========HEADER LIST===========")
    headerKeys = []
    for key in header.keys():
        print (key)
        headerKeys.append(key)

    #put keyDefs into set
    print ("=========FORMAT LIST===========")
    formatKeys = []
    for index, row in keyDefs.iterrows():
        print (row['keyword'])
        formatKeys.append(row['keyword'])

    #diff sets 
    diff1 = list(set(headerKeys) - set(formatKeys))
    print ("=========KEYWORDS DIFF (header - format)===========\n", diff1)

    diff2 = list(set(formatKeys) - set(headerKeys))
    print ("=========KEYWORDS DIFF (format - header)===========\n", diff2)

def compare_extended_headers(filepath1, filepath2):

    #wrap in try since some ext headers have been found to be corrupted
    try:

        hdus1 = fits.open(filepath1)
        hdus2 = fits.open(filepath2)

        if len(hdus1) != len(hdus2):
            print ("ERROR: Number if HDUs does not match.  Cannot compare.")
            return False

        for ext in range(0, len(hdus1)):
            if ext == 0: continue

            hdu1 = hdus1[ext]
            hdu2 = hdus2[ext]

            for key, val1 in hdu1.header.items():
                if not key: continue
                if key not in hdu2.header.keys():
                    print(f"WARN: EXT{ext} HDR1 key '{key}' not in HDR2")
                    continue
                val2 = hdu2.header[key]
                if val1 != val2:
                    print(f"WARN: EXT{ext} HDR1 key '{key}' value '{val1}' != '{val2}'")

            for key, val2 in hdu2.header.items():
                if not key: continue
                if key not in hdu1.header.keys():
                    print(f"WARN: EXT{ext} HDR2 key '{key}' not in HDR1")
                    continue
                val1 = hdu1.header[key]
                if val1 != val2:
                    print(f"WARN: EXT{ext} HDR2 key '{key}' value '{val2}' != '{val1}'")

    except Exception as e:
        print ("ERROR: ", e)
