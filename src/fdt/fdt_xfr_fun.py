"""
The transfer object
"""
import signal
import psutil
import subprocess
from pathlib import Path
from datetime import datetime

from dataclasses import dataclass


@dataclass(eq=False)
class XfrProcess:
    """
    Used to store consistent information while running and after restart.

    While running the subprocess object exists,  after a restart it cannot be
    recreated.
    """
    pid: int
    pkg_id: int
    start_time: datetime
    proc: subprocess.Popen | None = None


class FdtXfrFun:
    """
    the primary transfer module.
    """
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg_xfr = ctx.cfg['FDT_XFR']
        self.log = ctx.log

        self.db_pkg = ctx.db_pkg
        self.db_obs = ctx.db_obs

        # DTN cfg
        self.dtn_jar = self.cfg_xfr['dtn_jar']
        self.dtn_server = self.cfg_xfr['dtn_server']
        self.dtn_port = self.cfg_xfr['dtn_port']

        self.xfr_timeout = self.cfg_xfr['xfr_timeout']

        # general
        self.admin_email = self.ctx.cfg['GENERAL']['admin_email']

        self.stop_requested = False

        self.active_xfr = set()
        self.pid_finished = set()
        self.pid_error = set()

        # handle cntrl-c, kill <pid>
        signal.signal(signal.SIGINT, self.stop_handle)
        signal.signal(signal.SIGTERM, self.stop_handle)

    def stop_handle(self, signum, frame):
        """
        exit the process cleanly
        """
        self.log.info(f"Exiting, received signal {signum}.")
        self.stop_requested = True

    def start_transfer(self, pkg):
        """
        Spawn a transfer process.
        """
        pkg_id = pkg['pkg_id']
        tar_path = Path(f"{pkg['filepath']}/{pkg['filename']}")

        # start the transfer process
        proc = self.transfer_pkg(pkg_id, tar_path)
        if not proc:
            return

        # add pid and xfr_start_time (process creation time)
        pid = proc.pid

        proc_obj = psutil.Process(proc.pid)
        xfr_start_time = datetime.fromtimestamp(proc_obj.create_time())

        # create a new XfrProcess object to track the process
        xfr_obj = XfrProcess(
            pid=proc.pid, pkg_id=pkg_id, start_time=xfr_start_time, proc=proc
        )

        # track open process
        self.active_xfr.add(xfr_obj)

        # add the pid to pkg,  update status to TRANSFERRING
        self.db_pkg.update_pid(pkg_id, pid, xfr_start_time)

        # update the observations status = TRANSFERRING
        self.db_obs.update_status_by_pkg(pkg_id, "TRANSFERRING")

    def transfer_pkg(self, pkg_id, tar_path):
        """
        Transfer a tar package along with the .cfg file.

        pkg_id <int>: the database id of the package (tarfile)
        tar_path <Path>: the path to the tar file
        """
        # add cfg to pkg
        success = self.ctx.tar_fun.add_cfg(pkg_id, tar_path)
        if not success:
            self.log.error(f"Failed to add cfg file to {tar_path}")
            self.db_pkg.update_error(pkg_id, 'ERROR', 'CFG_ERROR')
            return None

        # open sentinel file
        complete_path = tar_path.with_suffix(".complete")
        complete_path.touch()

        # send tar followed by sentinel file --
        # sentinel is only sent if tar has no errors
        return self.transfer([str(tar_path), str(complete_path)])

    def transfer(self, file_list):
        """
        Transfer the files in the list,  one at a time in a single OS command
        so that the are in sequence.  Spawn a process to do the transfer to
        allow the loop to continue.

        send the files as one command using && to combine

        java -jar /koa/Fast-Data-Transfer/fdt-ver24 -c koadtn.ipac.caltech.edu -p 50750 -d INBOX test1.tar &&
        java -jar /koa/Fast-Data-Transfer/fdt-ver24 -c koadtn.ipac.caltech.edu -p 50750 -d INBOX test1.complete

        file_list <list>: list of files to transfer
        """
        cmd_base = [
            "/usr/bin/java", "-jar",
            self.dtn_jar,
            "-c", self.dtn_server,
            "-p", self.dtn_port,
            "-d", "INBOX"
        ]

        cmds = []

        # create a transfer command for each file in the filelist
        for file in file_list:
            cmds.append(" ".join(cmd_base + [file]))

        # combine them so they are sent in one OS process,  .complete last
        full_cmd = " && ".join(cmds)

        # detach from parent -> start_new_session=True
        proc = subprocess.Popen(full_cmd, shell=True, start_new_session=True)

        self.log.info(f"Transfer started, cmd: {full_cmd}, pid: {proc.pid}")

        return proc

    def chk_open_xfr(self):
        """
        Check if the process has ended
        """
        still_open = set()
        if self.active_xfr:
            self.log.info(f"Checking for open xfr: {self.active_xfr}")

        # check expired timeout
        expired = set()
        results = self.db_pkg.expired_transfers(self.xfr_timeout)
        if results:
            for result in results:
                pkg_id = result['pkg_id']
                expired.add(pkg_id)
                self.handle_timeout(pkg_id)

        for xfr_obj in self.active_xfr:
            if xfr_obj.pkg_id in expired:
                continue

            self.log.debug(f"Checking if {xfr_obj} is still open.")

            # subprocess info
            if xfr_obj.proc:
                result = xfr_obj.proc.poll()

                if result is None:
                    still_open.add(xfr_obj)
                    self.log.debug(f"still open")
                    continue

                if result == 0:
                    self.handle_complete_transfer(xfr_obj)
                else:
                    self.handle_failed_transfer(xfr_obj)
            # after restart
            else:
                if self.is_running(xfr_obj.pid, xfr_obj.start_time):
                    still_open.add(xfr_obj)
                else:
                    # process exited while we were offline
                    self.handle_complete_transfer(xfr_obj)

        self.log.debug(f"still open: {still_open}")
        self.active_xfr = still_open

    def is_running(self, pid, pid_start_time):
        """
        Confirm a pid is still running

        pid <int>: process id
        pid-start-time <datetime>: the time of process creation, same as the
                                   transfer start time.
        """
        try:
            proc = psutil.Process(pid)
        except psutil.NoSuchProcess:
            self.log.info(f"no proc object,  pid {pid} not running")
            return False

        # check is running and still the same process (same create time)
        if (proc.is_running() and
                abs(proc.create_time() - pid_start_time.timestamp()) < 1.0):

            return True

        return False

    def handle_complete_transfer(self, xfr_obj):
        """
        Finish up once the transfer completed.  The sentinel file needs to
        be transferred and the metrics updated.

        proc
        """
        self.log.info(f"Transfer complete: {xfr_obj}")
        pkg_id = xfr_obj.pkg_id
        xfr_end_time = datetime.now()

        # update the db_pkg,  set status = TRANSFERRED,  metrics
        self.db_pkg.update_transferred(pkg_id, xfr_end_time)

        # update the observations to transferred
        self.db_obs.update_status_by_pkg(pkg_id, "TRANSFERRED")

    def handle_failed_transfer(self, xfr_obj):
        """
        Handle a failed transfer.  Retry?  log?  email?
        """
        pkg_id = xfr_obj.pkg_id
        error_msg = f"TRANSFER_FAILED"

        self.db_pkg.update_error(pkg_id, 'ERROR', error_msg)

    def handle_missing_process(self, db_results):
        """
        On startup the process is gone.  Did it transfer or fail?
        """
        pkg_id = db_results['pkg_id']
        err_msg = f"PROCESS_NOT_RUNNING"

        self.log.debug(f"updating pkg error {err_msg}")

        # update the package to UNKNOWN
        self.db_pkg.update_error(pkg_id, 'UNKNOWN', err_msg)

        # update the observations to UNKNOWN,  will be updated on IPAC response
        self.db_obs.set_unknown(pkg_id, 'TRANSFERRING')

        return

    def handle_timeout(self, xfr_obj):
        """
        Handle the timeout of a transfer,  set to ERROR with short message.
        """
        self.log.error(f"transfer timeout: {xfr_obj}")
        err_msg = f"TRANSFER_TIMEOUT"

        self.db_pkg.update_error(xfr_obj.pkg_id, "ERROR", err_msg)

    def chk_on_startup(self):
        """
         Check to see if any pkgs are marked as TRANSFERRING
        """
        results = self.db_pkg.select_by_status("TRANSFERRING")

        for result in results:
            pid = result["xfr_pid"]
            pkg_id = result["pkg_id"]
            start_time = result["xfr_start_time"]
            self.log.info(f"On startup, checking if {pid} is TRANSFERRED")

            if not self.is_running(pid, start_time):
                self.log.error(
                    f"Missing process {pid}, start-up:  process "
                    f"{pid} is not running,  pkg_id: {pkg_id}")

                self.handle_missing_process(result)
                continue

            xfr_obj = XfrProcess(
                pid=pid, pkg_id=pkg_id,
                start_time=start_time
            )
            self.active_xfr.add(xfr_obj)

        results = self.db_pkg.select_by_status("CLOSE_REQUESTED")
        for result in results:
            filepath = result["filepath"]
            filename = result["filename"]
            filepath_obj = Path(f"{filepath}/{filename}")

            self.ctx.tar_fun.need_close(filepath_obj)



