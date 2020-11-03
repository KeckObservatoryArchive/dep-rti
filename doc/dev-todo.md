## HIGH PRIORITY
- Implement various command line options for archive.py
- Handle remaining errors/warns in dep/instrument/instr_[instr].
- Create a log file per KOAID?
- Cleanup dep.validate_fits() and dep.construct_filename()
- Decide if we use -tpx flag or a (-noxfr flag + dev config) to handle partial runs and/or test runs.
- Create independent DEP error monitoring script.
- monitor: Create metadata compare script for RTI vs nightly DEP
- monitor: Change monitor email time check to be per instrument
- monitor: Throttle max DEP processes based on server resources instead of hardcoded max=10?
- monitor: How will we recover if monitor is down and filepaths are not logged or inserted?
- monitor: Should monitor.py immediately copy file to staging before queuing?
- Add .fits to KOAID in DB?
- Implement handling for same filepath (ie renaming and updating 'stage_file' with _vN version)
- What are we doing with rejected/anc files? If status=INVALID, copy to /anc/ and rsync but do not notify IPAC?
- Implement basic missing program assignment
- More try/except to ensure processing finishes without crashing (ie set_koaimtyp)
- PyMysql is not thread safe: https://stackoverflow.com/questions/45535594/pymysql-with-django-multithreaded-application, https://github.com/PyMySQL/PyMySQL/issues/422
- Enum dep_status.arch_stat values? [QUEUED, PROCESSING, TRANSFERRING, TRANSFERRED, COMPLETE, INVALID, ERROR]
- DEIMOS FCS archive trigger (see old/dep_locate.py where some header keyword points to another file to archive)

 
## LOW PRIORITY
- Search TODOs in code
- Look at old DEP on github and ensure we got all hotfixes and changes since mid Sept
- Test PSFR (NIRC2)
- Test DRP (NIRC2, OSIRIS)
- Improve documentation
- Move common to processing base class and maybe get rid of common.py?
- Speed test caching importlib.  
- See if API calls are a considerable slowdown.
- Speed test all of code to find bottlenecks.
- Is there a fast gzip option?  Do a speed test vs internet speed.
- Design such that koa daemon can recieve code updates in realtime without restart.
- Do we want to merge archive.py and dep.py?
- Got this error once to stderr: "?RPC: Unable to send: monitor_server(kbds) __server_down__?."  Not sure if we can detect and log.
- Review usage of instrument.keymap and see if it needs improvement.
- Add "duplicate metadata keyword" check.  What to do? (ok if same val, otherwise ?)
- Improve logging, email reporting and error handling.
- Change keyword metadata defs to database tables?  Coordinate with IPAC.
- How do we keep track of new sdata dirs?  A: Added by Jchock and we aren't necessarily notified.  Need better system.
- See instr_lris.py for examples of condensed or streamlined functions that we can either apply to other instr_* files or create shared functions.


##NOTES:
- Keyword history query: echo "select to_timestamp(time),keyword,ascvalue from kbds where keyword='LOUTFILE' order by time desc limit 30;" | psql -h vm-history-1 -U k1obs -d keywordlog


## MISC IDEAS
- Do instrObj header fixes up front so we can just refer to things in the header as header['name']?
- Change back to instrument.py and subclasses as a FITS service class (ie not holding current fits file etc)?
- Command line option for dir removal and tpx removal if running manually?
- Create command line options to force program assignment by outdir or timerange.
- Processing instructions for typical admin steps (ie just running make_fits_extension_metadata_files)
- Make more functions as independent processing steps instead of dependent on self.
- Pull out metadata from DQA so it can be run as independent step after DQA? 


## REGRESSION TESTING
- Create test directory with collection of sample non-proprietary FITS files and corresponding "gold standard" DEP output for comparison.
- Create test script to validate DEP against sample FITS test directory.
- Use test data when API is called.  OR, create public route to API with config key.






