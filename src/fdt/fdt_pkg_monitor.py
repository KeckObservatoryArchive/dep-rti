"""
The FDT packaging monitor.
"""
import os
import sys
import time
import signal

from time import sleep
from pathlib import Path


class FdtPkgMonitor(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log
        self.pid = os.getpid()
        self.stop_requested = False

        # limit constants
        cfg_fdt = self.cfg['FDT_PROCESS']
        self.max_errors = cfg_fdt['max_errors']
        self.max_lock_retries = cfg_fdt['max_lock_retries']
        self.lock_chk_period = cfg_fdt['lock_chk_period']
        self.obs_watch_period = cfg_fdt['obs_watch_period']

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
        last_lock_chk = None

        # check for open packages and files left in processing state
        self.log.info("Starting FDT Package monitor.")

        err_retry = 0
        open_tarfiles = set()
        own_lock = False
        lock_retries = self.max_lock_retries
        last_lock_chk = 0
        my_id = None
        cleaned_up = False

        while not self.stop_requested:

            sleep(self.obs_watch_period)

            # Every period (seconds), verify the lock connection
            elapsed = time.monotonic() - last_lock_chk

            if not own_lock or elapsed >= self.lock_chk_period:
                own_lock, my_id = self.ctx.lock.check()
                last_lock_chk = time.time()

                # reset the error retry count at same interval,  used by max_errors
                err_retry = 0

                if own_lock:
                    self.log.info(f"Lock connected for {my_id}, pid: {self.pid} ")

            # allow max_lock_retries,  then exit
            if not own_lock:
                self.log.warning(
                    f"Lock not connected for {my_id}, pid: {self.pid}. "
                )
                lock_retries -= 1
                if lock_retries <= 0:
                    sys.exit(0)
                continue

            # cleanup on the first startup,  but after the lock is acquired
            if not cleaned_up:
                self.ctx.pkg_fun.startup_clean()
                cleaned_up = True

            # reset if the lock was obtained
            lock_retries = self.max_lock_retries

            self.log.info(f'Checking for PENDING Observations, pid {self.pid}.')
            try:
                open_tarfiles = self.process_observations(open_tarfiles)
            except Exception as err:
                # allow to continue but log the exception,  max errors = 5
                self.log.exception("Could not process observations.")
                if err_retry > self.max_errors:
                    # TODO should this send an alarm email?
                    self.stop_requested = True
                    self.log.error(f"Exiting,  max errors={self.max_errors}.")
                err_retry += 1

        # TODO on exit,  close tar files?
        # cleanup,  release lock,  close db connections
        self.ctx.lock.release()
        self.ctx.proc_conn.close()
        self.ctx.lock_conn.close()

    def process_observations(self, open_tarfiles):
        """
        Worker to do the packaging

        open_tarfiles <set><Path>: the open tarfile PATHs to check
        """

        observations = self.ctx.pkg_fun.chk_for_new_files()

        # check the tar file for size and temporal limits
        if open_tarfiles:
            open_tarfiles = self.chk_finalize_tarfiles(open_tarfiles)

        if not observations:
            return open_tarfiles

        already_checked = True

        for obs in observations:
            if self.stop_requested:
                break

            koaid = obs['koaid']

            self.log.debug(f'processing {obs}')

            # update the start time for metrics
            _ = self.ctx.db_obs.update_start_time(koaid)

            if not already_checked:
                open_tarfiles = self.chk_finalize_tarfiles(open_tarfiles)

            tarfile = self.ctx.pkg_fun.proc_obs(koaid)
            if tarfile:
                open_tarfiles.add(tarfile)

            # hasn't been checked since add
            already_checked = False

            # update the end time for metrics
            _ = self.ctx.db_obs.update_end_time(koaid)

        return open_tarfiles

    def chk_finalize_tarfiles(self, tarfile_set):
        """
        Only one tarfile within the instrument and level should only be open
        at one time.


        tarfile_set <set>: the tarfile PATHs to check
        """
        remove_files = set()
        for open_file in tarfile_set:
            tar_path = Path(open_file)
            if self.ctx.tar_fun.chk_finalize(tar_path):
                # tar was closed
                remove_files.add(open_file)

        tarfile_set -= remove_files

        return tarfile_set
