#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

#loop services
foreach service ( $argv )

	set PYTHON='/usr/local/anaconda/bin/python'
	set DEPDIR=`dirname $0`
	set LOGFILE="/koadata/dep-rti-$service.log"

	#todo: normally we would do start, not a restart, but using restart for development
	#$PYTHON $DEPDIR/src/manager.py monitor start --extra "$service" >> $LOGFILE
	set cmd="$PYTHON $DEPDIR/manager.py monitor restart --extra $service >>& $LOGFILE"
	echo $cmd
	eval "$cmd"

end