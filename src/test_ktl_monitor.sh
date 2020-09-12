#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc
/usr/local/anaconda/bin/python $HOME/test/dep-rti/src/manager.py test_ktl_monitor start --extra "kcwi"
