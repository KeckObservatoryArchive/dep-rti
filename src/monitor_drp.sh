#!/usr/local/bin/tcsh

#source environment variables so script will work from cron
source $HOME/.cshrc

#Usage
set all_instrs = ("deimos" "esi" "hires" "kcwi" "lris" "mosfire" "nirc2" "nires" "nirspec" "osiris")
if ($#argv == 0) then
	echo "\nUSAGE: Specify space-seperated list of instruments to restart or 'all'"
	echo "INSTRS: $all_instrs"
	echo "EXAMPLES:"
	echo "  monitor.sh kcwi nires"
	echo "  monitor.sh all"
	echo "\n"
	exit
endif

#get list
set instrs = $argv

if ($argv[1] == "all") then
	set instrs = ( $all_instrs )
endif

#loop instrs
foreach instr ( $instrs )

	set PYTHON='/usr/local/anaconda/bin/python'
	set DEPDIR=`dirname $0`
	set LOGFILE="/log/dep-drp-$instr.log"

	set cmd="$PYTHON $DEPDIR/manager.py monitor_drp restart --extra $instr >>& $LOGFILE"
	echo $cmd
	eval "$cmd"

end
