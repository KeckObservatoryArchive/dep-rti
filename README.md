Data Evaluation and Processing (DEP)
====================================

### Overview ###
DEP is the process and code by which Keck science data is processed, packaged and transmitted to the Keck Observatory Archive at NexSci.  This is the 3rd generation of DEP, redesigned for real-time archiving.

### Basic flow ###
- archive.py is called from command line with args, typically specifying a filepath to one file.
- A processing object is created for the appropriate instrument subclass.  Class relationships:
    - instr_[instr].py: Instrument subclass with instrument-specific processing functions. Inherits from Instrument. 
    - instrument.py: Instrument base class with processing funcs common to all instruments. Inherits from DEP.
    - dep.py: Base processing class (processing init, core proc funcs, flow, config, db conn).
- DEP.process() function is called which drives archiving of file from end to end calling each processing step in series. 
- NOTE: See each instrument subclass function run_dqa() for the core instrument-specific processing functions.


### Usage ###

Nominal usage:
```
python archive.py [instr] --filepath [filepath]
```

Reprocessing examples:
```
python archive.py [instr] --reprocess --starttime [starttime] --endtime [endtime]
python archive.py [instr] --reprocess --outdir [outdir]
python archive.py [instr] --reprocess --status ERROR
```
