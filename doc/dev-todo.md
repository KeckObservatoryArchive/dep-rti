## DEP


HIGH:
- Search TODOs in code and resolve or move remaining to JIRA tickets.  I've paired down a bunch, but there are still many minor todos.
- Look at old DEP on github and ensure we got all hotfixes and changes since mid Sep 2020.
- DEIMOS create_jpg_from_fits using up all memory. Allow only 1 DEIMOS DEP at once? Optimize function? Add more memory?  Don't create jpg? JPG turned off for now.
- Some 'duplicate keyword' errors are a result of other keyword service being down and utc not changing.  Maybe a check on ofname+utdatetime? If ofname differs, then use the current UT time to create the KOAID.  Check files are different too?  See, for example, KCWI 20201213 UT that had 54 duplicates.  This is a problem with pydep too.
- Cleanup or get rid of dep.validate_fits() and dep.construct_filename() (JM: No no longer need these since the monitor is giving us the full path and we know it is an instrument FITS file.) But, is this needed to catch mistakes with manual reprocessing runs?  
- Improve and expand regression testing (see /test/ directory).
- Create and update admin/dev/usage documentation (confluence and/or github).
- Monitor: PyMysql is not thread safe: https://stackoverflow.com/questions/45535594/, pymysql-with-django-multithreaded-application, https://github.com/PyMySQL/PyMySQL/issues/422
- Monitor: Throttle max DEP processes based on server resources instead of hardcoded max=10?
- Monitor: How will we recover if monitor is down?  Should execution client always append outfile + progid to a log file?  Should we have a code like scrubber report on differences?
- Find instances of "log.error" that are not using error reporting system.
- Complete Deimos.set_fcs_koaid() ?

LOW:
- Review which functions are marked "critical" in instr classes run_dqa() function. 
- Speed test all of code to find bottlenecks.
- See if API calls are a considerable slowdown. Create new proposalsAPI call that gets all info in one API call? Or direct query to prop DB?
- Review DEP.create_ext_meta.  Possible improvements needed.
- Monitor: Don't send error on KTL start/restarts if instr is offline
- Get rid of consistent metadata truncation warnings.  Some need bigger col size?
- Review usage of instrument.keymap and see if it needs improvement.
- Add "duplicate metadata keyword" check.  What to do? (ok if same val, otherwise ?)
- See instr_lris.py for examples of condensed or streamlined functions that we can either apply to other instr_* files or create shared functions.
- Create command line option in archive.py to override program assignment.
- Change cmd line to assume --reprocess and --transfer (keep confirm). Add --notransfer?
- Implement basic missing program assignment (revisit when execution client/etc worked out)
- Acknowledge column in dep_status to ignore warnings in report.
- Enum dep_status.arch_stat values? [QUEUED, PROCESSING, TRANSFERRING, TRANSFERRED, COMPLETE, INVALID, ERROR]


 
##NOTES:
- Keyword history query: echo "select to_timestamp(time),keyword,ascvalue from kbds where keyword='LOUTFILE' order by time desc limit 30;" | psql -h vm-history-1 -U k1obs -d keywordlog


## MISC IDEAS
- Do instrObj header fixes up front so we can just refer to things in the header as header['name']?
- Change back to instrument.py and subclasses as a FITS service class (ie not holding current fits file etc)?
- Make more functions as independent processing steps instead of dependent on self.









