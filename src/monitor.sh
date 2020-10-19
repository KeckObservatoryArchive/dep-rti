#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

set PYTHON='/usr/local/anaconda/bin/python'
set DEPDIR='/home/jriley/dev/koa/dep-rti'
set LOGFILE='/koadata/dep-rti.log'

#todo: normally we would do start, not a restart, but using restart for development
#$PYTHON $DEPDIR/src/manager.py monitor start --extra "kcwi" >> $LOGFILE

$PYTHON $DEPDIR/src/manager.py monitor restart --extra "kcwi" >>& $LOGFILE
#$PYTHON $DEPDIR/src/manager.py monitor restart --extra "kcwi"
