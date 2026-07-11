import sys
import logging
import argparse

from fdt.fdt_pkg_monitor import FdtPkgMonitor
from fdt.fdt_pkg_process import FdtPkgProcess
from fdt.fdt_pkg_context import FdtPkgContext


def reprocess_date_range(date_range, include_errors=False):
    """

    by default will reprocess all files in the range
      -- monitor is always watching for PENDING files

    set files in range to PENDING

    """
    if not include_errors:
       add_query = " and status not in ('ERROR')"
    else:
        add_query = None

    num = db_obj.update_obs_status_daterange(
        date_range[0], date_range[1], add=add_query
    )

    return num

def process_filepath(filepath):
    """

    loop through filepath to get KOAIDs

    loop through koaids to see if in the database

    if not in database,  add.
    if in database set to pending

        for each KOAID

        if exists in fdt_observations

            UPDATE status='PENDING', filepath=filepath,
            package_id=NULL

        else

            INSERT
                koaid
                filepath
                status='PENDING'
                filepath=filepath

    """
    return

def read_config(cfg_file):
    with open(cfg_file) as f:
        cfg = yaml.safe_load(f)

    return cfg

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'inst', help='The name of the instrument to monitor.', required=True
    )
    parser.add_argument(
        '--lev', help='The KOA level to watch (lev0, lev1, lev2).', required=True
    )
    parser.add_argument(
        '--filepath', help='Optional: filepath of files to process.'
    )
    parser.add_argument(
        "--date-range", nargs=2, metavar=("START", "END"),
        help="Optional: Start and end date"
    )
    parser.add_argument(
        "--include-errors", action="store_true",
        help="(Flag) to be used to process error files."
    )
    parser.add_argument(
        "--transfer-now", action="store_true",
        help="(Flag) Close current tar file and transfer."
    )
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    log = logging.getLogger(__name__)

    args = parse_args()
    """
    read config
    """
    inst = args.inst
    lev = args.lev
    if args.filepath:
        file_path = args.filepath
    else:
        # TODO this should be config filepath
        file_path = "."

    # read the config into a dict
    cfg = read_config(f'{args.cfg_path}/fdt_config.yaml')

    try:
        ctx = FdtPkgContext(inst, lev, cfg, log)
    except Exception as err:
        print(err)
        sys.exit(1)

    # package processing object
    proc_obj = FdtPkgProcess(ctx)

    # infinite loop to monitor the database for new pending observations
    fdt_pkg_monitor = FdtPkgMonitor(ctx, proc_obj)


    # TODO,  this needs the date to only clean up the date
    proc_obj.startup_clean(ctx.cfg['tarfiles']['filepath'])

    # handle reprocessing and date range
    if args.transfer_now:
        ctx.db_pkg.change_status("OPEN", "CLOSED")

    elif args.date_range:
        if args.reprocess_errors:
            include_errors = True
        else:
            include_errors = False

        reprocess_date_range(
            args.date_range, args.reprocess, log,
            errors_ok=include_errors
        )

    elif args.filepath:
        process_filepath(args.filepath)


