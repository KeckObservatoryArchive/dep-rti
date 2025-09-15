#!/bin/bash

# Source environment variables so script will work from cron
source "$HOME/.bashrc"

# Usage
all_instrs=("deimos" "esi" "hires" "kcwi" "lris" "mosfire" "nirc2" "nires" "nirspec" "osiris" "scales")

if [ "$#" -eq 0 ]; then
  echo -e "\nUSAGE: Specify space-separated list of instrs to restart or 'all'"
  echo "INSTRS: ${all_instrs[*]}"
  echo "EXAMPLES:"
  echo "  monitor.sh kcwi nires"
  echo "  monitor.sh all"
  echo -e "\n"
  exit 1
fi

# get list
instrs=("$@")

# get the UT date
UT_DATE=$(date -u +%Y%m%d)

if [ "${instrs[0]}" == "all" ]; then
  instrs=("${all_instrs[@]}")
fi

# loop instrs
for instr in "${instrs[@]}"; do
  PYTHON='/usr/local/anaconda/bin/python'
  DEPDIR="$(dirname "$0")"
  LOGFILE="/log/dep-drp-${instr}-${UT_DATE}.log"

  cmd="$PYTHON $DEPDIR/manager.py monitor_drp restart --extra $instr"
  echo "$cmd"
  $cmd >> "$LOGFILE" 2>&1

done

