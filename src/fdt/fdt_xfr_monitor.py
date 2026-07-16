"""
The monitor of the package table to start transfers
"""

import signal
from time import sleep

class FdtXfrMonitor(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.xfr = ctx.xfr_fun

        self.db_pkg = ctx.db_pkg
        self.monitor_period = self.cfg['FDT_XFR']['monitor_period']

        self.stop_requested = False

        # handle cntrl-c, kill <pid>
        signal.signal(signal.SIGINT, self.stop_handle)
        signal.signal(signal.SIGTERM, self.stop_handle)

    def stop_handle(self, signum, frame):
        """
        exit the process cleanly
        """
        self.log.info(f"Exiting, received signal {signum}.")
        self.stop_requested = True


    def run(self):
        """
        Monitor the fdt_packages database table for any packages that are closed.
        """

        # check on startup for TRANSFERRING packages
        self.xfr.chk_on_startup()

        while not self.stop_requested:

            # check the open processes
            self.xfr.chk_open_xfr()

            sleep(self.monitor_period)

            # find CLOSED packages
            ready_pkgs = self.db_pkg.ready_to_transfer()

            if not ready_pkgs:
                continue

            for pkg in ready_pkgs:
                self.xfr.start_transfer(pkg)

            # check for processes finished
            self.xfr.chk_open_xfr()

