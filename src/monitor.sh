#!/bin/bash

# Source environment variables so script will work from cron
source "$HOME/.bashrc"

# Usage
all_services=("kfcs" "kblue" "kred" "deimos" "deifcs" "hrs" "kpf")

if [ "$#" -eq 0 ]; then
  echo -e "\nUSAGE: Specify space-separated list of services to restart or 'all'"
  echo "SERVICES: ${all_services[*]}"
  echo "EXAMPLES:"
  echo "  monitor.sh kfcs kbds"
  echo "  monitor.sh all"
  echo -e "\n"
  exit 1
fi

services=("$@")

# Get the UT date
UT_DATE=$(date -u +%Y%m%d)

if [ "${services[0]}" == "all" ]; then
  services=("${all_services[@]}")
fi

# Loop through services
for service in "${services[@]}"; do
  PYTHON='/usr/local/anaconda/bin/python'
  DEPDIR="$(dirname "$0")"
  LOGFILE="/log/dep-rti-${service}-${UT_DATE}.log"

  cmd="$PYTHON $DEPDIR/manager.py monitor restart --extra $service"
  echo "$cmd"
  $cmd >> "$LOGFILE" 2>&1

done

