## Tag and Release Process

Before tagging:
- Commit config.ini with new "DEP_VERSION" value

To tag with github: 
- Go to 'Code' => 'Release' tab
- Click "Create a new release"
- Use "v0.0.0" versioning

To release to server:
- Use build account
- cd to release folder
- Checkout as version folder: git clone https://github.com/KeckObservatoryArchive/koa-rti.git ./v0.1.0
- Create/edit "config.live.ini".
- TEST!
- change "default" symbolic link: ln -s v0.1.0 release

Verify cron job:
- A cron job should be running on the server for each instrument using 'koaadmin' user account.  Example:



## Creating Metadata Format File
If you need to create or make changes to the keyword metadata definitions:

- Get latest spreadsheet definition file (currently as Google docs)
- Export columns in this order: keyword, dataType, colSize, allowNull
- Make sure KOAID is first keyword row
- Save to repo as /metadata/keywords.format.<INSTR> (ie "keywords.format.NIRES")


## DEP keyword mapping explained
- instrument.py contains a dictionary var self.keywordMap with key value pairs.  
- An entry's key is how we will reference a certain keyword in the code.
- An entry's value is the actual keyword string to look for in the FITS header.  
- An entry's value can instead be an array denoting an order list of possible keyword strings to look for.
- An instrument subclass (ie instr_nires.py) can add or overwrite keywordMap entires
- Instrument.py now has a get_keyword and set_keyword functions that use keywordMap to access and modify keywords.
- A default return value can be specified in get_keyword.



## Regression Testing
TODO




