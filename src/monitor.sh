#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

#Usage
set all_services = ("kfcs" "kbds" "deimosplus" "deifcs" "hiccd")
if ($#argv == 0) then
	echo "\nUSAGE: Specify space-seperated list of services to restart or 'all'"
	echo "SERVICES: $all_services"
	echo "EXAMPLES:"
	echo "  monitor.sh kfcs kbds"
	echo "  monitor.sh all"
	echo "\n"
	exit
endif

#get list
set services = $argv

if ($argv[1] == "all") then
	set services = ( $all_services )
endif

#loop services
foreach service ( $services )

	set PYTHON='/usr/local/anaconda/bin/python'
	set DEPDIR=`dirname $0`
	set LOGFILE="/koadata/dep-rti-$service.log"

	set cmd="$PYTHON $DEPDIR/manager.py monitor restart --extra $service >>& $LOGFILE"
	echo $cmd
	eval "$cmd"

end
