# Quickstart guide

pyDEP is a software written specifically for Keck's archiving process.  A typical user of this software will need to be moderately familiar with this process and KOA in general.  Certain parts of this software were written to address particularly thorny issues arising from differences in data and keywords between the Keck instruments and lack of clear data concerning data program assignment.

pyDEP is a port from a much older version written mostly in IDL and shell scripts.  Many of the concepts, names, processing stages, intermediate output, and architecture were carried over.  Improvements were made in certain areas as time permitted and are still ongoing.  


## Concepts

(todo: Explain stages and intermediary output files.)

(todo: Explain program assignment issue.)


## Configure pyDEP
**(NOTE: You must currently be on the Keck internal network to run this correctly.)**

- Copy config.ini to config.live.ini 
- Edit config.live.ini
    - Set RUNTIME->DEV = 1
    - Define TELAPI url
    - Define ROOTDIR to point to your own test output dir
    - Set ADMIN_EMAIL to your email
    - Comment out all vars in KOAXFR section


## Create test directories and test data
- Create the ROOTDIR you defined in your config file.
- Create a test dir that contains sample fits files.


## Run pyDEP
- **IMPORTANT: If testing, be sure to set the command line tpx flag to 0 so you do not insert/update the koatpx table!**
- NOTE: pyDEP normally looks for fits files in specific server directories and that were created or modified in the past 24 hours defined by the date you are archiving.  To override this for testing, we use the --modtimeOverride and --searchDir options below.  Your test fits files header data should match the instrument and date you are running (check DATE-OBS keyword).


- Example run commands:
```
python dep_go.py [instr] [utDate] [tpx] [procStart] [procStop]  --modtimeOverride=1  --searchDir=[fits dir]

python dep_go.py ESI 2019-08-29 0 obtain dqa --modtimeOverride=1 --searchDir=/Users/jriley/test/sdata/ESI/ 
```

## Caveats to not running on a koa server:
    - You will not have access to the MET files used in the 'add' step.  Weather keywords will be set to 'none'.
    - The email report feature will only work if you are running a mail server.
    - You will not be able to run the koaxfr step.
