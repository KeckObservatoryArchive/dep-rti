Data Evaluation and Processing (DEP)
====================================

### Overview ###
DEP is the process and code by which Keck science data is processed, packaged and transmitted to the Keck Observatory Archive at NexSci.


### Processing Steps ###
The DEP process is divided into the following logical steps:

1. **obtain**: Retrieve the program information from the telescope schedule
2. **locate**: Locate the instrument FITS files written to disk in the 24 hour period
3. **add**: Add the focus and weather logs
4. **dqa (data quality assess)**: assess the raw FITS files and add metadata keywords
5. **lev1**: level 1 data reduction
6. **tar**: tar and zip data for transfer
7. **koaxfr**: transfer the data to NExScI


### Usage ###
The DEP code is designed to run at the Keck Observatory intranet.  However, a Continuous Integration Testing option is included in this repository with test FITS files and options to run a limited test of the code.  See the "/cit/" folder for further instructions.
