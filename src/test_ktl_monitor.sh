#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

set PYTHON='/usr/local/anaconda/bin/python'
set DEPDIR='/home/jriley/dev/koa/dep-rti'
set LOGFILE='/usr/local/home/koarti/test/dep_out/dep-rti.log'

#todo: normally we would do start, not a restart, but using restart for development
#$PYTHON $DEPDIR/src/manager.py test_ktl_monitor start --extra "kcwi" >> $LOGFILE
$PYTHON $DEPDIR/src/manager.py test_ktl_monitor restart --extra "kcwi" >> $LOGFILE