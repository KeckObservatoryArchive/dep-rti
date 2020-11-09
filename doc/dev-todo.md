## DEP
- Convert remaining instr_[instr].py errors/warns to use custom log_error function.
- Correcty mark critical functions in instr classes run_dqa()function.
- Create DEP error monitoring script to look at dep_status for errors to email.
- Implement basic missing program assignment.
- Cleanup dep.validate_fits() and dep.construct_filename()
- Create a log file per KOAID?
- Enum dep_status.arch_stat values? [QUEUED, PROCESSING, TRANSFERRING, TRANSFERRED, COMPLETE, INVALID, ERROR]
- DEIMOS FCS archive trigger (see old/dep_locate.py where some header keyword points to another file to archive)
- What about PSFR (NIRC2) and DRP (NIRC2, OSIRIS) hooks?

## MONITOR
- Fix ktl service restart so we don't keep getting RPC error messages.
- Don't send error on KTL start/restarts if instr is offline
- Change monitor email time check to be per instrument?
- How will we recover if monitor is down and filepaths are not logged or inserted?  Should execution client always append outfile + progid to a log file?
- PyMysql is not thread safe: https://stackoverflow.com/questions/45535594/, pymysql-with-django-multithreaded-application, https://github.com/PyMySQL/PyMySQL/issues/422
- Throttle max DEP processes based on server resources instead of hardcoded max=10?
 
## LOW PRIORITY
- Search TODOs in code
- Add .fits to KOAID in DB?
- Look at old DEP on github and ensure we got all hotfixes and changes since mid Sept
- Improve documentation
- Move remaining common.py to processing base class.
- Speed test caching importlib.  
- See if API calls are a considerable slowdown.
- Speed test all of code to find bottlenecks.
- Is there a fast gzip option?  Do a speed test vs internet speed.
- Do we want to merge archive.py and dep.py?
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






