#!/bin/bash

# Source environment variables so script will work from cron
source "$HOME/.bashrc"

# Usage
if [ "$#" -eq 0 ]; then
  echo -e "\nUSAGE: Specify service to restart"
  echo "EXAMPLES:"
  echo "  archive.sh kcwi_blue kcwi_red"
  echo "  archive.sh mosfire"
  echo -e "\n"
  exit 1
fi

# Loop through services
services=("$@")
for service in "${services[@]}"; do
  PYTHON='/usr/local/anaconda/bin/python'
  DEPDIR="$(dirname "$0")"
  cmd="$PYTHON $DEPDIR/manager.py archive_only restart --extra $service"
  echo "$cmd"
  $cmd > /dev/null 2>&1
done
