(NOTE: This is an uber detailed list of development TODOs.)
(NOTE: High level todos also listed in github projects page.)  


## HIGH PRIORITY
- Have instrument.py inherit from another base processing class.  Then you just create the obj and call .process()
- Move common to processing base class and maybe get rid of common.py?
- Remove unneeded code for RTI (dep_[step].py)
- Analyze common.py and see which functions are DEP specific enough to go into dep.py or instrument.py vs true common funcs.
- See if API calls are a considerable slowdown.
- Speed test all of code to find bottlenecks.
- Search TODOs in code
- Improve documentation


## LOW PRIORITY
- Review usage of keywordMap and see if it needs improvement.
- Add "duplicate metadata keyword" check.  What to do? (ok if same val, otherwise ?)
- Improve logging, email reporting and error handling.
- Change keyword metadata defs to database tables?  Coordinate with IPAC.
- How do we keep track of new sdata dirs?  A: Added by Jchock and we aren't necessarily notified.  Need better system.
- See instr_lris.py for examples of condensed or streamlined functions that we can either apply to other instr_* files or create shared functions.
- log all queries in db_conn to reduce code bloat?

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






