## DEP

HIGH:
- Search TODOs in code
- Look at old DEP on github and ensure we got all hotfixes and changes since mid Sep 2020.
- fix reset_status_record... it is clearing creation_time
- Put service name on debug print in on_new_file
- Add code to ignore certain strings in monitor/monitor_config. (ie /xdchange/ for HIRES)
- Create new script (or mod check_dep_status_errors.py) to do a daily report which would include warnings and invalids.
- Change cmd line to assume --reprocess and --transfer (keep confirm). Add --notransfer?
- Create a log file per KOAID? (and/or mark all logs with dbid)
- DEIMOS triggering same FCS image file which results in many duplicate IDs.
- DEIMOS create_jpg_from_fits using up all memory. Allow only 1 DEIMOS DEP at once? Optimize function? Add more memory?  Don't create jpg? JPG turned off for now.
- Some 'duplicate keyword' errors are a result of other keyword service being down and utc not changing.  Maybe a check on ofname+utdatetime? If ofname differs, then use the current UT time to create the KOAID.  Check files are different too?  See, for example, KCWI 20201213 UT that had 54 duplicates.  This is a problem with pydep too.
- Cleanup or get rid of dep.validate_fits() and dep.construct_filename() (JM: No no longer need these since the monitor is giving us the full path and we know it is an instrument FITS file.) Is this needed to catch mistakes with manual reprocessing runs?  

LOW:
- Improve and expand regression testing (see /test/ directory).
- Create admin/dev documentation.
- Log all errors and warnings to seperate database table (in addition to logging).  Change error reporting code accordingly.
- Review critical functions marked in instr classes run_dqa() function. 
- See if API calls are a considerable slowdown. Create new proposalsAPI call that gets all info in one API call? Or direct query to prop DB?
- Implement basic missing program assignment (revisit when execution client/etc worked out)
- What about PSFR (NIRC2) and DRP (NIRC2, OSIRIS) hooks?
- Get rid of metadata truncation warnings.
- Speed test all of code to find bottlenecks.
- Review usage of instrument.keymap and see if it needs improvement.
- Add "duplicate metadata keyword" check.  What to do? (ok if same val, otherwise ?)
- Improve logging, email reporting and error handling.
- See instr_lris.py for examples of condensed or streamlined functions that we can either apply to other instr_* files or create shared functions.
- Create command line option in archive.py to override program assignment.

IDEAS:
- Acknowledge column in dep_status to ignore warnings in report.
- Enum dep_status.arch_stat values? [QUEUED, PROCESSING, TRANSFERRING, TRANSFERRED, COMPLETE, INVALID, ERROR]


## MONITOR
- Setup simple ktl logging with less noise for comparison?
- !Fix ktl service restart so we don't keep getting RPC error messages.
- !Try to replicate issue of multiple service instances causing multiple callback (force code to del using self and see if that replicates issue).  Do we need to delete the callback as well?  See "remove" param in keyword.callback !!!
- Don't send error on KTL start/restarts if instr is offline
- Change monitor email time check to be per instrument?
- How will we recover if monitor is down and filepaths are not logged or inserted?  Should execution client always append outfile + progid to a log file?
- !PyMysql is not thread safe: https://stackoverflow.com/questions/45535594/, pymysql-with-django-multithreaded-application, https://github.com/PyMySQL/PyMySQL/issues/422
- !Throttle max DEP processes based on server resources instead of hardcoded max=10?

 
##NOTES:
- Keyword history query: echo "select to_timestamp(time),keyword,ascvalue from kbds where keyword='LOUTFILE' order by time desc limit 30;" | psql -h vm-history-1 -U k1obs -d keywordlog


## MISC IDEAS
- Do instrObj header fixes up front so we can just refer to things in the header as header['name']?
- Change back to instrument.py and subclasses as a FITS service class (ie not holding current fits file etc)?
- Make more functions as independent processing steps instead of dependent on self.









