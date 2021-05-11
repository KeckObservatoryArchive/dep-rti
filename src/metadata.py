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

log = logging.getLogger("koa_dep")

def make_metadata(keywordsDefFile, metaOutFile, searchdir=None, filepath=None, 
                  extraMeta=dict(), dev=False, keyskips=[], create_md5=False):
    """
    Creates the archiving metadata file as part of the DQA process.

    @param keywordsDefFile: keywords format definition input file path
    @type keywordsDefFile: string
    @param metaOutFile: metadata output file path
    @type metaOutFile: string
    @param searchdir: directory for finding FITS files and writing output files
    @type searchdir: string
    @param extraMeta: dictionary of any extra key val pairs not in header
    @type extraMeta: dictionary
    """

    #open keywords format file and read data
    log.info('metadata.py reading keywords definition file: {}'.format(keywordsDefFile))
    keyDefs = pd.read_csv(keywordsDefFile, sep='\t')
    try:
        keyDefs = format_keyDefs(keyDefs)
    except Exception as err:
        keyDefs = format_keyDefs(keyDefs)
        msg = 'keywordsDefFile {0} not formatted err: {1} skipping'.format(keyDefs, err)
        log.error(msg)
        if not dev:
            raise Exception(msg)

    #create initial file with header
    create_metadata_file(metaOutFile, keyDefs)

    #track warning counts
    warns = {'type': 0, 'truncate': 0, 'minValue': 0, 'maxValue': 0, 'discreteValues': 0}

    #get all fits files
    log.info('metadata.py searching fits files in dir: {}'.format(searchdir))
    fitsFiles = []
    if searchdir:
        for path in Path(searchdir).rglob('*.fits'):
            fitsFiles.append(str(path))
    if filepath:
        fitsFiles.append(filepath)
    if len(fitsFiles) == 0:
        log.info(f'No fits file(s) found')
    for fitsFile in sorted(fitsFiles):
        extra = {}
        baseName = os.path.basename(fitsFile)
        if baseName in extraMeta:
            extra = extraMeta[baseName]
        log.info("Creating metadata record for: " + fitsFile)
        warns = add_fits_metadata_line(fitsFile, metaOutFile, keyDefs, extra, warns, dev, keyskips)

    #warn only if counts
    for warn, numWarns in warns.items():
        if numWarns == 0:
            continue
        msg = 'metadata.py: found {0} warnings of type {1}'.format(numWarns, warn)
        log.warning(msg)

    #md5sum option
    if create_md5: 
        create_md5_checksum_file(metaOutFile)

    return True

def format_keyDefs(keyDefs):
    '''renames and type declarations for metadata table'''
    keyDefs = keyDefs.rename(columns={'FITSKeyword': 'keyword', 'MetadataDatatype': 'metaDataType', 'NullsAllowed':'allowNull', 'MetadataWidth': 'colSize', 'MinValue': 'minValue', 'MaxValue': 'maxValue'})
    keyDefs = keyDefs.dropna(axis=0, subset=['keyword'])
    keyDefs = keyDefs[keyDefs['Source'].astype(str) != 'NExScI']
    keyDefs['colSize'] = keyDefs['colSize'].astype(int)
    #keyDefs['minValue'] = keyDefs['minValue'].astype(float)
    #keyDefs['maxValue'] = keyDefs['maxValue'].astype(float)
    return keyDefs

def create_md5_checksum_file(metaOutFile):
    #create md5 sum
    assert 'metadata.table' in metaOutFile, 'metaOutFile must be metadata.table file'
    md5OutFile = metaOutFile.replace('.table', '.md5sum')

    metaOutPath = os.path.dirname(metaOutFile)
    # make_dir_md5_table(metaOutPath, ".metadata.table", md5OutFile)
    with open(md5OutFile, 'w') as fp:
        md5 = hashlib.md5(open(metaOutFile, 'rb').read()).hexdigest()
        bName = os.path.basename(metaOutFile)
        fp.write(md5 + '  ' + bName + '\n')
        fp.flush()


def create_metadata_file(filename, keyDefs):
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
        #todo: add units?
        for index, row in keyDefs.iterrows():
            out.write('|' + ''.ljust(row['colSize']))
        out.write("|\n")

        for index, row in keyDefs.iterrows():
            nullStr = '' if (row['allowNull'] == "N") else "null"
            out.write('|' + nullStr.ljust(row['colSize']))
        out.write("|\n")
        out.flush()


def add_fits_metadata_line(fitsFile, metaOutFile, keyDefs, extra, warns, dev, keyskips):
    """
    Adds a line to metadata file for one FITS file.
    """

    #get header object using astropy
    header = fits.getheader(fitsFile)
    #check keywords
    check_keyword_existance(header, keyDefs, dev, keyskips, extra)
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
                    log.error('metadata check: Could not read header keyword (' + fitsFile + '): ' + keyword)
                    val = 'null'
            elif keyword in extra:
                val = extra[keyword]
            else: 
                val = 'null'
                if dev: log.error('metadata check: Keyword not found in header (' + fitsFile + '): ' + keyword)

            #special check for val = fits.Undefined
            if isinstance(val, fits.Undefined):
                val = 'null'

            #special check for 'NaN' or '-Nan'
            if val in ('NaN', '-NaN'):
                val = 'null'

            #check keyword val and format
            try:
                val, warns = check_keyword_val(keyword, val, row, warns)
            except Exception as err:
                msg = 'Exception for metaOutFile {0} keyword: {1} val: {2}. Error: {3}'.format(os.path.basename(metaOutFile), keyword, val, err)
                log.error(msg)
                if not dev:
                    raise Exception(msg)

            #write out val padded to size
            out.write(' ')
            out.write(str(val).ljust(colSize))
            out.flush()
        out.write("\n")
    return warns


def check_keyword_existance(header, keyDefs, dev=False, keyskips=[], extra={}):

    #get simple list of keywords
    keyDefList = []
    for index, row in keyDefs.iterrows():
        keyDefList.append(row['keyword'])        

    #find all keywords in header that are not in metadata file
    skips = ['SIMPLE', 'COMMENT', 'PROGTL1', 'PROGTL2', 'PROGTL3'] + keyskips
    for keywordHdr in header:
        if not keywordHdr: continue  #blank keywords can exist
        if keywordHdr not in keyDefList and not is_keyword_skip(keywordHdr, skips):
            if dev: log.warning('metadata.py: header keyword "{}" not found in metadata definition file.'.format(keywordHdr))

    #find all keywords in metadata def file that are not in header
    skips = ['PROGTITL', 'PROPINT']
    assert keyDefs.iloc[0].keyword == "KOAID", "First column must be KOAID"
    for index, row in keyDefs.iterrows():
        keyword = row['keyword']
        if keyword not in header and keyword not in skips and row['allowNull'] == "N" and keyword not in extra:
            if dev: log.warning('metadata.py: non-null metadata keyword "{}" not found in header.'.format(keyword))

def check_null(val, allowNull):
    if (val == 'null' or val == '') and (allowNull == 'N'):
        raise Exception('metadata check: incorrect "null" value found for non-null keyword {}'.format(keyword))            

def fix_value(val, metaDataType, keyword):
    '''Some known value conversions we must do.'''
    if (metaDataType == 'char'):
        if isinstance(val, bool):
            if   (val == True):  val = 'T'
            elif (val == False): val = 'F'
        elif isinstance(val, int) and val == 0:
            val = ''
            log.info(f'metadata check: {keyword}: found integer 0, expected {metaDataType}. KNOWN ISSUE. SETTING TO BLANK!')
    return val

def check_and_set_char_length(val, warns,  colSize, metaDataType, keyword, dev=False):
    length = len(str(val))
    if (length > colSize):
        if (metaDataType == 'double'): 
            log.warning('metadata check: char length of {} greater than column size of {} ({}={}).  TRUNCATING.'.format(length, colSize, keyword, val))
            warns['truncate'] += 1
            val = truncate_float(val, colSize)
        else: 
            log.warning('metadata check: char length of {} greater than column size of {} ({}={}).  TRUNCATING.'.format(length, colSize, keyword, val))
            warns['truncate'] += 1
            val = str(val)[:colSize]
    return val, warns

def is_none(x):
    '''checks that floats and ints are None or nan'''
    nones = (None, nan)
    if not isinstance(x, (int, float)):
        return False
    return (x in nones) or (isnan(x))

def skip_if_input_has_none(method):
    @wraps(method)
    def do_if_no_nones(*args):
        noneInArgs = any([is_none(x) for x in args])
        if not noneInArgs:
            return method(*args)
        else: 
            return args[1] # returns warns
    return do_if_no_nones

def convert_type(val, vtype):
    '''convert to either integer or float if vtype specifies'''
    if vtype=='integer':
        return int(val)
    elif vtype=='double':
        return float(val)
    else:
        return val
@skip_if_input_has_none
def check_min_range(val, warns, minVal, vtype, keyword):
    try:
        if val < convert_type(minVal, vtype):
            log.error(f'metadata check: {keyword} val {val} < minVal {minVal}')
            warns['minValue'] += 1
    except Exception as err:
        print(err)
    return warns

@skip_if_input_has_none
def check_max_range(val, warns, maxVal, vtype, keyword):
    if val > convert_type(maxVal, vtype):
        log.error(f'metadata check: {keyword} val {val} > maxVal {maxVal}')
        warns['maxValue'] += 1
    return warns

@skip_if_input_has_none
def check_discrete_values(val, warns, valStr, keyword):
    '''Discrete value string can be JSON or comma-separated.'''
    try:
        valSet = json.loads(valStr)
    except Exception as e:
        valSet = valStr.split(',')

    valSet = [x.strip().lower() for x in valSet]
    if not val.lower() in valSet:
        log.error(f'metadata check: {keyword} val "{val}" not in {valSet}')
        warns['discreteValues'] += 1
    return warns

def check_value_type(val, warns, mtype, keyword):
    '''Check that we can cast to expected type.'''
    try:
        if   mtype == 'integer':  val = int(val)
        elif mtype == 'double':   val = float(val)
        elif mtype == 'date':     val = datetime.datetime.strptime(val, '%Y-%m-%d')
        elif mtype == 'time':     val = datetime.datetime.strptime(val, '%H:%M:%S.%f')
        elif mtype == 'datetime': val = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
        elif mtype == 'angle':    val = Angle(val, au.deg)
    except:
        log.error(f"metadata_check: {keyword} val '{val}' is not type {mtype}")
        warns['type'] += 1
    return warns

def check_keyword_val(keyword, val, fmt, warns, dev=False):
    '''
    checks keyword for correct type and proper value.
    '''
    #specific error, udf values that we should convert to "null"
    errvals = ['#### Error ###']
    if (val in errvals):
        val = 'null'

    #deal with null, blank vals
    check_null(val, fmt['allowNull'])
    if (val == 'null' or val == '') and (fmt['allowNull'] == 'Y'):
        return val, warns

    #basic checks of type and length
    mtype = fmt['InputFormat'] if fmt['InputFormat'] else fmt['metaDataType']
    val = fix_value(val, fmt['metaDataType'], keyword)
    if fmt['ValidateFormat'].upper() == 'Y':
        warns = check_value_type(val, warns, mtype, keyword)
    val, warns = check_and_set_char_length(val, warns, fmt['colSize'], fmt['metaDataType'], fmt['keyword'], dev)
    val = convert_type(val, fmt['metaDataType'])

    #check range and discrete values?
    if fmt['CheckValues'].upper() == 'Y':
        # check if val is angle in degrees
        if not pd.isnull(fmt['minValue']) and mtype == 'angle':
            ang = Angle(val, au.deg)
            minAng = Angle(fmt['minValue'], au.deg)
            maxAng = Angle(fmt['maxValue'], au.deg)
            if ang < minAng:
                log.error(f'metadata check: {keyword} val {ang} < minVal {minAng}')
                warns['maxValue'] += 1
            if ang > maxAng:
                log.error(f'metadata check: {keyword} val {ang} > maxVal {maxAng}')
                warns['maxValue'] += 1
        else:
            warns = check_min_range(val, warns, fmt['minValue'], fmt['metaDataType'], keyword)
            warns = check_max_range(val, warns, fmt['maxValue'], fmt['metaDataType'], keyword)
            warns = check_discrete_values(val, warns, fmt['DiscreteValues'], keyword)

    return val, warns

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

def header_keyword_report(keywordsDefFile, fitsFile):

    #read keywords format file and fits file
    keyDefs = pd.read_csv(keywordsDefFile, sep='\t')
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
