"""
Command line interface to interact with the FDT package and transfer monitors
"""
import re
import sys
import logging
import argparse

from pathlib import Path
from datetime import datetime, timedelta

from fdt import fdt_utils
from fdt.fdt_pkg_context import FdtPkgContext


def re_pkg_date_range(date_range, db_obj, include_errors, only_errors):
    """
    Re-package files in a daterange.

    Default is to re-pakcage all files in the range
      -- change files to PENDING,  which will initiate the monitor

    date_range <list><str> -- start and end date
    db_obj <obj> -- the database object
    include_errors <bool> -- whether to include errors or not
    only_errors <bool> -- whether to only include errors or not
    """
    add_query = None
    if not include_errors:
       add_query = " and status not in ('ERROR')"

    if only_errors:
        add_query = " and status='ERROR'"

    num = db_obj.repackage_by_daterange(
        date_range[0], date_range[1], add=add_query
    )

    if num == 0:
        print(f"No matching observations found for {get_cmdline_opts()}")
    else:
        print(f"Processing {num} files.")

    return num

def re_transfer_pkgs(date_range, db_pkg):
    """
    Re-transfer packages based on date range.

    date_range <list><str>: start and end date
    db_pkg <obj>> the package database object
    """
    start_dt = datetime.strptime(date_range[0], "%Y%m%d")
    end_dt = datetime.strptime(date_range[1], "%Y%m%d") + timedelta(days=1)

    num = db_pkg.reset_status_by_daterange(start_dt, end_dt)

    if num == 0:
        print(f"No matching packages found for {get_cmdline_opts()}")
    else:
        print(f"Processing {num} packages.")

    return num


def process_filepath(inst_prefixes, filepath, db_obj):
    """
    Process files in a filepath with the instrument prefix defined in the
    configuration file that match the instrument.  All matching files in
    the filepath will be added to the package and transferred to KOA.
    """

    koaid_set = set()

    koaid_re = re.compile(r"^[A-Z]+\.\d{8}\.\d{5}\.\d{2}")

    filepath_obj = Path(filepath)

    # find all <inst-prefix>*.fits
    for prefix in inst_prefixes:
        for path in filepath_obj.glob(f"{prefix}.*.fits"):
            match = koaid_re.match(path.name)
            if match:
                koaid_set.add(match.group(0))

    in_db = db_obj.search_koaids(list(koaid_set))
    koaid_updated = []
    for obs in in_db:
        koaid = obs['koaid']
        num = db_obj.set_replacement_path(koaid, filepath)
        if num > 0:
            koaid_updated.append(koaid)

    for koaid in koaid_set:
        if koaid in koaid_updated:
            continue
        num = db_obj.insert_obs(koaid, filepath_obj.resolve(), 'PENDING')

    log.info(f"Processing {len(koaid_set)} files")

    return


def parse_args(allowed_insts):
    """
    Parse Arguments

    - Instrument (required)
    - level (required)
    - Date range,  with or without errors
    - Errors only within a date range
    - start transfer of all open packages for Inst / Level
    - re-initiate transfers in ERROR

    return <ArgParse Object>:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--inst', help='(Required) The name of the instrument to monitor.',
        required=True, choices=allowed_insts
    )
    parser.add_argument(
        '--lev', help='(Required) The KOA level to watch lev(0, 1, 2).',
        required=True, choices=(0, 1, 2)
    )
    parser.add_argument(
        '--filepath', help='(Optional) Filepath of files to process.'
    )
    parser.add_argument(
        "--date-range", nargs=2, metavar=("START", "END"),
        help="(Optional) Start and end date"
    )
    parser.add_argument(
        "--include-errors", action="store_true",
        help="(Flag) to be used to include error files in date-range."
    )
    parser.add_argument(
        "--only-errors", action="store_true",
        help="(Flag) to be used to process only error files in date-range."
    )
    parser.add_argument(
        "--transfer-now", action="store_true",
        help="(Flag) Close current tar file and transfer."
    )
    parser.add_argument(
        "--re-transfer", action="store_true",
        help="(Flag) Re-transfer all in ERROR for a date range."
    )
    args = parser.parse_args()

    return args


def get_cmdline_opts():
    """
    Helper to parse the defined command line options.
    """
    opts = ", ".join(f"{k}={v}" for k, v in vars(args).items() if v)
    return opts


if __name__ == '__main__':
    log = logging.getLogger(__name__)

    # read the config into a dict
    cfg_path = "fdt/fdt_config.live.yaml"

    cfg = fdt_utils.read_config(cfg_path)

    allowed_insts = cfg["GENERAL"]["instruments"]

    args = parse_args(allowed_insts)
    inst = args.inst
    lev = args.lev

    try:
        ctx = FdtPkgContext(inst, lev, cfg_path, log)
    except Exception as err:
        print(err)
        sys.exit(1)

    # handle reprocessing and date range
    if args.transfer_now:
        ctx.db_pkg.change_status("OPEN", "CLOSE_REQUESTED")

    elif args.date_range:
        if args.re_transfer:
            re_transfer_pkgs(args.date_range, ctx.db_pkg)
        else:
            re_pkg_date_range(
                args.date_range, ctx.db_obs,
                args.include_errors, args.only_errors
            )

    elif args.filepath:
        inst_prefixes = ctx.cfg[inst]['inst_prefixes']
        process_filepath(inst_prefixes, args.filepath, ctx.db_obs)


