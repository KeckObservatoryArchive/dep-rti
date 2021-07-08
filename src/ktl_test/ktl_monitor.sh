#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

set services = ("kfcs" "kbds" "nids" "nsds" "deimosplus" "deifcs" "hiccd")
foreach service ( $services )

	set PYTHON='/usr/local/anaconda/bin/python'
	set DEPDIR='/home/filer2/jriley/dev/koa/dep-rti/src/ktl_test'

	echo $service
	$PYTHON $DEPDIR/manager.py ktl_monitor restart --extra $service 

end