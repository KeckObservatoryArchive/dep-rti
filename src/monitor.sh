#!/bin/bash

# Source environment variables so script will work from cron
source "$HOME/.bashrc"

# Usage
all_services=("kfcs" "kblue" "kred" "deimos" "deifcs" "hrs" "kpf")

run_mode="split"
server_name="monitor_only"

show_usage() {
  # echo -e "\nUSAGE: monitor.sh [--run-mode split|combined] <service1 service2 ...|all>"
  echo -e "\nUSAGE: monitor.sh [--run-mode split|combined] <service1 service2 ...>"
  # echo "SERVICES: ${all_services[*]}"
  echo "EXAMPLES:"
  echo "  monitor.sh kbds"
  # echo "  monitor.sh all"
  # echo "  monitor.sh --run-mode split all"
  # echo "  monitor.sh --run-mode combined all"
  echo -e "\n"
}

if [ "$#" -eq 0 ]; then
  show_usage
  exit 1
fi

if [ "$1" == "--run-mode" ]; then
  if [ -z "$2" ]; then
    echo "ERROR: --run-mode requires a value: split or combined"
    show_usage
    exit 1
  fi

  run_mode="$2"
  shift 2
fi

case "$run_mode" in
  split)
    server_name="monitor_only"
    ;;
  combined)
    server_name="monitor"
    ;;
  *)
    echo "ERROR: Invalid --run-mode value '$run_mode'. Use split or combined."
    show_usage
    exit 1
    ;;
esac

if [ "$#" -eq 0 ]; then
  show_usage
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
#  LOGFILE="/log/dep-rti-${service}-${UT_DATE}.log"

  cmd="$PYTHON $DEPDIR/manager.py $server_name restart --extra $service"
  echo "$cmd"
#  $cmd >> "$LOGFILE" 2>&1
  $cmd >> /dev/null 2>&1

done

