"""
The FDT packaging monitor.
"""
import os
import sys
import time
import signal

from time import sleep


class FdtPkgMonitor(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.pid = os.getpid()
        self.stop_requested = False

        # limit constants
        cfg_general = self.cfg['GENERAL']
        self.max_errors = cfg_general['max_errors']
        self.max_lock_retries = cfg_general['max_lock_retries']
        self.lock_chk_period = cfg_general['lock_chk_period']

        # monitor period
        cfg_fdt = self.cfg['FDT_PKG']
        self.monitor_period = cfg_fdt['monitor_period']

        # lock
        self.last_lock_chk = 0
        self.own_lock = False
        self.lock_retries = self.max_lock_retries

        # errors
        self.err_retry = 0

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
        Run the infinite loop until SIGINT or SIGTERM is received
        or the process is terminated.
        """

        self.log.info("Starting FDT Package monitor.")

        cleaned_up = False

        while not self.stop_requested:

            sleep(self.monitor_period)

            if not self.chk_lock():
                continue

            # cleanup on the first startup,  but after the lock is acquired
            if not cleaned_up:
                self.ctx.pkg_fun.startup_clean()
                cleaned_up = True

            # check the tar file for size and temporal limits
            self.ctx.pkg_fun.chk_finalize_tars()

            # check for new observations
            self.log.debug(f'Checking for PENDING Observations, pid {self.pid}.')
            try:
                self.process_observations()
            except Exception as err:
                # allow to continue but log the exception,  max errors = 5
                self.log.exception("Could not process observations.")
                if self.err_retry >= self.max_errors:
                    self.stop_requested = True
                    self.log.error(f"Exiting,  max errors={self.max_errors}.")
                self.err_retry += 1

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

            # reset the error retry count at same interval,  used by max_errors
            self.err_retry = 0

            if self.own_lock:
                self.log.info(f"Lock connected for {my_id}, pid: {self.pid} ")

            # check for errors at same frequency (does not check stalled)
            self.ctx.utils.chk_for_errors(self.ctx, self.ctx.db_obs)

        # allow max_lock_retries,  then exit
        if not self.own_lock:
            self.log.warning(f"Lock not connected for {my_id}, pid: {self.pid}. ")
            self.lock_retries -= 1
            if self.lock_retries <= 0:
                print('Packaging Monitor is locked,  '
                      'is another process running?.')
                sys.exit(0)
            return False

        # reset if the lock was obtained
        self.lock_retries = self.max_lock_retries

        return True

    def process_observations(self):
        """
        Worker to do the packaging
        """

        observations = self.ctx.pkg_fun.chk_for_new_files()

        if not observations:
            return

        tar_checked = True

        for obs in observations:
            if self.stop_requested:
                break

            koaid = obs['koaid']

            self.log.debug(f'processing {obs}')

            # update the start time for metrics
            _ = self.ctx.db_obs.update_start_time(koaid)

            if not tar_checked:
                self.ctx.pkg_fun.chk_finalize_tars()

            self.ctx.pkg_fun.proc_obs(koaid)

            # hasn't been checked since add
            tar_checked = False

            # update the end time for metrics
            _ = self.ctx.db_obs.update_end_time(koaid)


