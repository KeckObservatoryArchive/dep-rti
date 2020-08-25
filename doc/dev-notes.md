
## DEP keyword mapping explained
- instrument.py contains a dictionary var self.keywordMap with key value pairs.  
- An entry's key is how we will reference a certain keyword in the code.
- An entry's value is the actual keyword string to look for in the FITS header.  
- An entry's value can instead be an array denoting an order list of possible keyword strings to look for.
- An instrument subclass (ie instr_nires.py) can add or overwrite keywordMap entires
- Instrument.py now has a get_keyword and set_keyword functions that use keywordMap to access and modify keywords.
- A default return value can be specified in get_keyword.



## Processing Notes

How to run DEP on test data:
- Copy fits data to a test directory (use -p option to preserve timestamps)
- Clone DEP from git
- Create config.live.ini from config.ini
  - Edit ROOTDIR to point to your output directory for these test runs.
  - Edit SEARCH_DIR to point to the test directory to search for FITS files
  - Edit ADMIN_EMAIL to go to you.
  - Comment out all the KOAXFR section so you don't accidentally send stuff to IPAC
  - Optional: If you don't have appropriate timestamps on the fits files, turn on MODTIME_OVERRIDE.
- Run DEP with TPX flag off and up to tar step (don't koaxfr): python dep_go.py MOSFIRE 2019-01-20 0 obtain tar



## (old) koatpx DB table summary:

	utdate         | date         | 
	instr          | varchar(10)  | 
	pi             | varchar(68)  | 
	files          | int(11)      | 
	files_arch     | int(11)      | 
	size           | float        | 
	sdata          | varchar(15)  | 
	ondisk_stat    | varchar(10)  | 
	ondisk_time    | varchar(15)  | 
	arch_stat      | varchar(10)  | 
	arch_time      | varchar(15)  | 
	metadata_stat  | varchar(10)  | 
	metadata_time  | varchar(15)  | 
	dvdwrit_stat   | varchar(10)  | 
	dvdwrit_time   | varchar(15)  | 
	dvdsent_stat   | varchar(10)  | 
	dvdsent_time   | varchar(15)  | 
	dvdsent_init   | char(3)      | 
	dvdsent_com    | varchar(80)  | 
	dvdstor_stat   | varchar(10)  | 
	dvdstor_time   | varchar(15)  | 
	dvdstor_init   | char(3)      | 
	dvdstor_com    | varchar(80)  | 
	tpx_stat       | varchar(10)  | 
	tpx_time       | varchar(15)  | 
	comment        | varchar(250) | 
	start_time     | varchar(15)  | 
	metadata_time2 | varchar(15)  | 
	sci_files      | int(11)      | 
	drpSent        | varchar(15)  | 
	lev1_stat      | varchar(10)  | 
	lev1_time      | varchar(15)  | 



