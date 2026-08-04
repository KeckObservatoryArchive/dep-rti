"""
The commandline interface to start the FDT package monitor.
"""

import sys
import logging
import argparse
from datetime import datetime

from fdt import fdt_utils
from fdt.fdt_pkg_monitor import FdtPkgMonitor
from fdt.fdt_pkg_context import FdtPkgContext

log = logging.getLogger(__name__)


def parse_args(allowed_insts):
    """
    Parse command line arguments.

        -- inst - instrument (required)
        -- lev - data processing level (required)
        -- file-path - file path of data to package (optional)
        -- tar-path - tar path (optional)

    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--inst', help='The name of the instrument to monitor.', required=True,
        choices = allowed_insts,
    )
    parser.add_argument(
        '--lev', help='The KOA level to watch (lev0, lev1, lev2).', required=True
    )
    parser.add_argument(
        '--file-path', help='Change the file path from the default location'
    )
    parser.add_argument(
        '--tar-path', help='Change the file path from the default location'
    )

    return parser.parse_args()


if __name__ == '__main__':
    log = logging.getLogger(__name__)

    # config file
    cfg_file = f'fdt/fdt_config.live.yaml'
    cfg = fdt_utils.read_config(cfg_file)

    allowed_insts = cfg["GENERAL"]["instruments"]

    # get the command line arguments
    args = parse_args(allowed_insts)

    inst = args.inst
    lev = args.lev

    # set-up logging
    log_level = getattr(
        logging, cfg["LOGGING"]["pkg_level"].upper(), logging.INFO
    )
    log_date = datetime.now().strftime("%Y%m%d")
    log_dir = cfg['LOGGING']['log_dir']
    logging.basicConfig(
        filename=f"{log_dir}/{inst}/fdt_pkg_{inst}_lev{lev}_{log_date}.log",
        level=log_level,
        format=(
            "%(asctime)s %(levelname)-8s "
            "%(filename)s:%(funcName)s:%(lineno)d - %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # set the context used throughout
    try:
        ctx = FdtPkgContext(
            inst, lev, cfg_file, log,
            filepath=args.file_path, tar_path=args.tar_path
        )
    except Exception as err:
        print(err)
        sys.exit(1)

    # only one lock allowed
    ctx.lock.acquire()

    # infinite loop to monitor the database for PENDING observations
    fdt_pkg_monitor = FdtPkgMonitor(ctx)

    # start the loop
    fdt_pkg_monitor.run()




