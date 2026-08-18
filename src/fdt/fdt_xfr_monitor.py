"""
The monitor of the package table to start transfers
"""
import os
import sys
import time
import signal
from time import sleep


class FdtXfrMonitor(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.xfr = ctx.xfr_fun

        cfg_general = self.cfg['GENERAL']
        self.max_lock_retries = cfg_general['max_lock_retries']
        self.lock_chk_period = cfg_general['lock_chk_period']
        self.pid = os.getpid()

        self.db_pkg = ctx.db_pkg
        self.monitor_period = self.cfg['FDT_XFR']['monitor_period']

        # lock
        self.own_lock = False
        self.last_lock_chk = 0
        self.lock_retries = self.max_lock_retries

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
        cleaned_up = False

        while not self.stop_requested:

            sleep(self.monitor_period)

            if not self.chk_lock():
                continue

            # cleanup on the first startup,  but after the lock is acquired
            if not cleaned_up:
                # check on startup for TRANSFERRING packages
                self.xfr.chk_on_startup()
                cleaned_up = True

            # check the open processes
            self.xfr.chk_open_xfr()

            # find CLOSED packages
            ready_pkgs = self.db_pkg.ready_to_transfer()

            if not ready_pkgs:
                continue

            for pkg in ready_pkgs:
                self.xfr.start_transfer(pkg)

        # cleanup,  release lock,  close db connections
        self.ctx.lock.release()
        self.ctx.proc_conn.close()
        self.ctx.lock_conn.close()

    def chk_lock(self):
        """
        Connect and check if the lock is owned by current process.

        Retry if lock not owned.
        """
        my_id = None
        # Every period (seconds), verify the lock connection
        elapsed = time.time() - self.last_lock_chk
        if not self.own_lock or elapsed >= self.lock_chk_period:
            self.own_lock, my_id = self.ctx.lock.check()
            self.last_lock_chk = time.time()

            if self.own_lock:
                self.log.info(f"Lock connected for {my_id}, pid: {self.pid} ")

            # check for errors at same frequency (does not check stalled)
            self.ctx.utils.chk_for_errors(self.ctx, self.db_pkg)

        # allow max_lock_retries,  then exit
        if not self.own_lock:
            self.log.warning(f"Lock not connected for {my_id}, pid: {self.pid}.")
            self.lock_retries -= 1
            if self.lock_retries <= 0:
                print('Transfer Monitor is locked,  is another '
                      'xfr process running?')
                sys.exit(0)
            return False

        # reset if the lock was obtained
        self.lock_retries = self.max_lock_retries

        return True

