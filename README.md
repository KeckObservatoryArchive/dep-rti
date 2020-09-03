Data Evaluation and Processing (DEP)
====================================

### Overview ###
DEP is the process and code by which Keck science data is processed, packaged and transmitted to the Keck Observatory Archive at NexSci.

### Basic flow ###
- archive.py is called from cmd line with args, typically a filepath to one file.
- A processing object is created for the appropriate instrument subclass.  Class relationships:
    - instr_[instr].py: Instrument specific processing subclass inheriting from Instrument (processing funcs specific to instrument)
    - instrument.py: Instrument processing class inheriting from DEP (processing funcs common to all instruments)
    - dep.py: Base processing class (processing init, core proc funcs, flow, config, db conn)
- DEP.process() function is called which drives archiving of file from end to end.  
- Instrument specific processing contained in run_dqa() functions.


### Usage ###

python archive.py [instr] --filepath [filepath]
