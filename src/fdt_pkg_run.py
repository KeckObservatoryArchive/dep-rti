import sys
import logging
import argparse
from datetime import datetime

from fdt import fdt_utils
from fdt.fdt_pkg_monitor import FdtPkgMonitor
from fdt.fdt_pkg_context import FdtPkgContext


log = logging.getLogger(__name__)


def parse_args():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--inst', help='The name of the instrument to monitor.', required=True,
        choices = ["SCALES"],
    )
    parser.add_argument(
        '--lev', help='The KOA level to watch (lev0, lev1, lev2).', required=True
    )
    parser.add_argument(
        '--cfg-path', help='Change the configuration file path from the '
                           'default of the current directory', default='fdt'
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

    # get the command line arguments
    args = parse_args()

    inst = args.inst
    lev = args.lev

    cfg_file = f'{args.cfg_path}/fdt_config.live.yaml'
    cfg = fdt_utils.read_config(cfg_file)

    log_date = datetime.now().strftime("%Y%m%d")
    log_dir = cfg['GENERAL']['log_dir']
    logging.basicConfig(
        filename=f"{log_dir}/fdt_pkg_{args.inst}_lev{args.lev}_{log_date}.log",
        # level=logging.INFO,
        level=logging.DEBUG,
        format=(
            "%(asctime)s %(levelname)-8s "
            "%(filename)s:%(funcName)s:%(lineno)d - %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        ctx = FdtPkgContext(
            inst, lev, cfg_file, log,
            filepath=args.file_path, tar_path=args.tar_path
        )
    except Exception as err:
        print(err)
        sys.exit(1)

    ctx.lock.acquire()

    # infinite loop to monitor the database for new pending observations
    fdt_pkg_monitor = FdtPkgMonitor(ctx)

    # start the loop
    fdt_pkg_monitor.run()




