#!/bin/bash

LEV=$1
INST=$2
MODE=$3
PYTHON="/usr/local/anaconda/bin/python3"

if [ -z "$LEV" ] || [ -z "$INST" ] || [ -z "$MODE" ]; then
    echo "Usage: $0 <level 0|1|2> <instrument SCALES> <xfr|pkg>"
    exit 1
fi

# script in same location as this script
WORKDIR="$(cd "$(dirname "$0")" && pwd)"

case "$MODE" in
    xfr)
        SCRIPT="fdt_xfr_run.py"
        ;;
    pkg)
        SCRIPT="fdt_pkg_run.py"
        ;;
    *)
        echo "Invalid mode: $MODE"
        echo "Usage: $0 <level> <instrument> <xfr|pkg>"
        exit 1
        ;;
esac

CMD="$PYTHON $SCRIPT --lev $LEV --inst $INST"

if ! pgrep -f "$SCRIPT --lev $LEV --inst $INST" > /dev/null
then
    echo "$(date): starting $CMD"

    cd "$WORKDIR" || exit 1

    $CMD &

    echo "$(date): started $CMD, PID $!"
fi

