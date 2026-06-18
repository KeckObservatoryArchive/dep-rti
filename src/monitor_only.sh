#!/bin/bash

# Source environment variables so script will work from cron
source "$HOME/.bashrc"

# Usage

if [ "$#" -eq 0 ]; then
  echo -e "\nUSAGE: Specify space-separated list of services to restart"
  echo "EXAMPLES:"
  echo "  monitor_only.sh kcwi_blue kcwi_red"
  echo "  monitor_only.sh mosfire"
  echo -e "\n"
  exit 1
fi

services=("$@")

# Loop through services
for service in "${services[@]}"; do
  PYTHON='/usr/local/anaconda/bin/python'
  DEPDIR="$(dirname "$0")"

  cmd="$PYTHON $DEPDIR/manager.py monitor_only restart --extra $service"
  echo "$cmd"
  $cmd > /dev/null 2>&1

done
