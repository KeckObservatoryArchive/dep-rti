Data Evaluation and Processing (DEP) + Real-Time Ingestion (RTI)
====================================

### Overview ###
Keck's Data Evaluation and Processing (DEP) is the code by which Keck science data is processed, packaged and transmitted to the Keck Observatory Archive at IPAC.  This is the 3rd generation of DEP, redesigned for real-time ingestion (RTI) archiving.

### Setup and Configuration ###
- Clone this code repo to a location accessible by the KOA real-time processing server.
- Copy config.ini to config.live.ini and define the various configuration sections:
    - DATABASE: Login credentials for live or dev database
    - API: Various KECK API URLs needed
    - INSTRUMENTS: Define output processing dir location and other instrument specific subprocess routes.
    - KOAXFR: Define API routes and locations for delivery to IPAC. 

### Usage ###
Nominally, DEP is run via a data monitoring daemon (see monitor.py) which continuously monitors instrument-specific KTL keywords indicating a new datafile has been written to disk and is ready to be archived.  The monitor will spawn an instance of DEP for that single datafile. 

You can also run DEP via the command line, whether that be to process new file(s) or reprocess existing records.

File(s) processing examples::
```
python archive.py [instr] --filepath [filepath]
python archive.py [instr] --files [glob pattern]
```

Reprocessing examples:
```
python archive.py [instr] --reprocess --starttime [starttime] --endtime [endtime]
python archive.py [instr] --reprocess --status [status]
python archive.py [instr] --reprocess --dbid [database id]
```

### Basic class inheritance ###
When DEP is run, a processing object is created for the appropriate instrument subclass, whose class inheritance is outlined below:
    - DEP: Base processing class (processing init, core funcs, flow, config, db conn).
    - Instrument: Instrument base class with processing funcs common to all instruments. Inherits from DEP.
    - [instrument subclass]: Instrument subclass with instrument-specific processing functions. Inherits from Instrument. 
- DEP.process() function is called which drives archiving of file from end to end calling each processing step in series. 
- NOTE: See each instrument subclass function run_dqa() for the core instrument-specific processing functions.


