import re
import sys
import logging
import argparse

from fdt.fdt_pkg_context import FdtPkgContext

from pathlib import Path

def reprocess_date_range(date_range, db_obj, include_errors=False):
    """

    by default will reprocess all files in the range
      -- monitor is always watching for PENDING files

    set files in range to PENDING

    """
    if not include_errors:
       add_query = " and status not in ('ERROR')"
    else:
        add_query = None

    num = db_obj.update_status_by_daterange(
        date_range[0], date_range[1], add=add_query
    )

    return num

def process_filepath(inst_prefixes, filepath, db_obj):
    """

    loop through filepath to get KOAIDs

    loop through koaids to see if in the database
        -- if in the database,  set to PENDING

    add records for the remaining files.
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
        print(f"koaid: {koaid}.")
        num = db_obj.set_replacement_path(koaid, filepath)
        if num > 0:
            koaid_updated.append(koaid)

    for koaid in koaid_set:
        if koaid in koaid_updated:
            continue
        num = db_obj.insert_obs(koaid, filepath_obj.resolve(), 'PENDING')

    return


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--inst', help='The name of the instrument to monitor.', required=True
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

    # read the config into a dict
    cfg_path = f'fdt/fdt_config.live.yaml'

    try:
        ctx = FdtPkgContext(inst, lev, cfg_path, log)
    except Exception as err:
        print(err)
        sys.exit(1)

    # handle reprocessing and date range
    if args.transfer_now:
        ctx.db_pkg.change_status("OPEN", "CLOSED")

    elif args.date_range:
        if args.include_errors:
            include_errors = True
        else:
            include_errors = False

        reprocess_date_range(
            args.date_range, ctx.db_obs, include_errors=include_errors
        )

    elif args.filepath:
        inst_prefixes = ctx.cfg[inst]['inst_prefixes']
        process_filepath(inst_prefixes, args.filepath, ctx.db_obs)


