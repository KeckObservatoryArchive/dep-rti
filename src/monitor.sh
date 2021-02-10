#!/usr/local/bin/tcsh
set instr=$argv[1]
set instrupper=`echo "$instr" | tr '[a-z]' '[A-Z]'`

#source environment variables so script will work from cron
source $HOME/.cshrc

set PYTHON='/usr/local/anaconda/bin/python'
set DEPDIR='/usr/local/home/koarti/dev/koa/dep-rti'
set LOGFILE="/koadata/$instrupper/dep-rti-$instr.log"

#todo: normally we would do start, not a restart, but using restart for development
#$PYTHON $DEPDIR/src/manager.py monitor start --extra "$instr" >> $LOGFILE
set cmd="$PYTHON $DEPDIR/src/manager.py monitor restart --extra $instr >>& $LOGFILE"
echo $cmd
eval "$cmd"
