# Quickstart development guide


## About DEP
DEP is a software written specifically for Keck's archiving process.  A typical user of this software will need to be moderately familiar with this process and KOA in general.  

This DEP is a significant reimplementation of a prior version that was designed for full night ingestion of data files.  This version is leaner and redesigned to work as "realtime" ingestion and archive of data files.


## Configure and Setup DEP (for development mode)
**(NOTE: You must currently be on the Keck internal network to run this correctly.)**

- Copy config.ini to config.live.ini and edit necessary configurations
    - Define RAWDIR and instrument ROOTDIR directories locally for test output files.
    - Point your database configs to the test database server (to ensure we don't update real tables)
- Create a test dir that contains sample fits files.


## Run DEP

Example run commands:
```
python archive.py [instr] --filepath [filepath]

python archive.py ESI --filepath /user/testdata/sdata707/esieng/2019aug30/e190830_0019.fits --tpx 0 --koaxfr 0
```

**IMPORTANT: "--tpx 0" will ensure no DB inserts or updates occur**
**IMPORTANT: "--koaxfr 0" will ensure no transfer to IPAC occurs**


## Caveats to not running on the correct KOA server:
- You will not have access to the MET files and EPICS archiver, so some keywords such as weather keywords will be set to 'none'.
- The email report feature will only work if you are running a mail server.
- You will not be able to run the koaxfr step.


## Explanation of files and classes
- archive.py: Handle command line arguments and calling DEP processing object.
- dep.py: Base processing class (processing init, common proc funcs, proc flow, config, db conn).
- instrument.py: Base instrument class, common funcs for instrument processing.
- instr_[instr].py: Instrument specific subclass.
- monitor.py: Realtime monitoring daemon that looks at KTL keywords to find new files to archive.
