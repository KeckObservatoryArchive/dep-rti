#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

#Usage
set all_services = ("kcwi_fcs" "kcwi_blue" "kcwi_red" "deimos" "deimos_fcs" "hires" "kpf")
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

# get the UT date
set UT_DATE=`date -u +%Y%m%d`

if ($argv[1] == "all") then
	set services = ( $all_services )
endif

#loop services
foreach service ( $services )

	set PYTHON='/usr/local/anaconda/bin/python'
	set DEPDIR=`dirname $0`
  set LOGFILE="/log/dep-rti-$service-$UT_DATE.log"

	set cmd="$PYTHON $DEPDIR/manager.py monitor restart --extra $service >>& $LOGFILE"
	echo $cmd
	eval "$cmd"

end
