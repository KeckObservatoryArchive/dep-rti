"""
The transfer object
"""

import yaml
import signal
import psutil
import subprocess
from time import sleep
from pathlib import Path
from datetime import datetime

from dataclasses import dataclass

@dataclass
class XfrProcess:
    """
    Used to store consistent information while running and after restart.

    While running the subprocess object exists,  after a restart it cannot be
    recreated.
    """
    pid: int
    pkg_id: int
    start_time: datetime
    proc: "subprocess.Popen | None" = None


class FdtXfrFun:
    """
    the primary transfer module.
    """
    def __init__(self, ctx):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.log = ctx.log

        self.db_pkg = ctx.db_pkg
        self.db_obs = ctx.db_obs
        self.pkg_watch_period = self.cfg['FDT_XFR']['pkg_watch_period']

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

        self.db_pkg.update_pid(pkg_id, pid, xfr_start_time)

        # update the status = TRANSFERRING
        self.db_pkg.update_status_by_pkg(pkg_id, "TRANSFERRING")

    def transfer_pkg(self, pkg_id, tar_path):
        """
        Transfer a tar package along with the .cfg file.

        pkg_id <int>: the database id of the package (tarfile)
        tar_path <Path>: the path to the tar file
        """

        # create the cfg file to transfer with the tar file
        cfg_path = tar_path.with_suffix(".cfg")
        self.create_cfg(pkg_id, cfg_path, tar_path)

        return self.transfer([str(tar_path), str(cfg_path)])

    def transfer_sentinel(self, tar_path):
        """
        Transfer a sentinel file.

        pkg_id <int>: the database id of the package (tarfile)
        tar_path <Path>: the path to the tar file
        """
        # open and send the sentinel file
        complete_path = tar_path.with_suffix(".complete")
        complete_path.touch()

        return self.transfer([str(complete_path)])


    def transfer(self, file_list):
        """
        spawn a process to do the transfer

        something needs to wait for it to complete to update the status and metrics

        java -jar /koa/Fast-Data-Transfer/fdt-ver24 -c koadtn.ipac.caltech.edu -p 50750 -d INBOX file.tar
        java -jar /koa/Fast-Data-Transfer/fdt-ver24 -c koadtn.ipac.caltech.edu -p 50750 -d INBOX file.complete

        file_list <list>: list of files to transfer
        """
        cmd_base = [
            "java", "-jar",
            "/koa/Fast-Data-Transfer/fdt-ver24",
            "-c", "koadtn.ipac.caltech.edu",
            "-p", "50750",
            "-d", "INBOX"
        ]

        cmd = cmd_base + file_list

        # detach from parent -> start_new_session=True
        proc = subprocess.Popen(cmd, start_new_session=True)

        return proc

    def create_cfg(self, pkg_id, cfg_path, tar_path):
        """
        Create the cfg file to accompany the tar file.  The file contains
        three sections,  instrument,  level,  and koaids.

        pkg_id <int>: the database id of the package (tarfile)
        cfg_path <Path>: the path to the cfg file
        tar_path <Path>: the path to the tar file
        """

        # find the koaids included in the tar file
        koaids = self.ctx.tar_fun.included_koaids(tar_path)

        # confirm the database says the same
        db_koaids = {row["koaid"] for row in self.db_obs.koaids_in_pkg(pkg_id)}

        # log the difference if it exists
        koaid_diff = koaids - db_koaids
        if koaid_diff:
            self.log.error(f"{pkg_id} has differences with db: {koaid_diff}.")

        cfg_data = {
            "instrument": self.ctx.inst,
            "ingesttype": self.ctx.lev_str,
            "filelist": koaids
        }

        cfg_path.write_text(
            yaml.safe_dump(
                cfg_data, sort_keys=False, default_flow_style=False, indent=2
            )
        )

    def chk_open_xfr(self):
        """
        Check if the process has ended
        """
        still_open = set()

        for xfr_obj in self.active_xfr:

            # the subprocess information is known
            if xfr_obj.proc:
                result = xfr_obj.proc.poll()

                if result is None:
                    still_open.add(xfr_obj)
                    continue

                if result == 0:
                    self.handle_complete_transfer(xfr_obj)
                else:
                    self.handle_failed_transfer(xfr_obj, result)
            # after restart
            else:
                if self.is_running(xfr_obj.pid, xfr_obj.start_time):
                    still_open.add(xfr_obj)
                else:
                    # process exited while we were offline
                    self.handle_missing_process(xfr_obj)

        self.active_xfr = still_open


    def is_running(self, pid, pid_start_time):
        """
        Confirm is a pid still running

        pid <int>: process id
        pid-start-time <datetime>: the time of process creation, same as the
                                   transfer start time.
        """
        proc = psutil.Process(pid)

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
        # update the db_pkg,  set status = TRANSFERRED,  metrics

        # update db_obs,  set status = TRANSFERRED, metrics

        # send the sentinel file

        return

    def handle_failed_transfer(self, proc, error_code):
        """
        Handle a failed transfer.  Retry?  log?  email?
        """
        return

    def handle_missing_process(self, proc):
        """
        On startup the process is gone.  Did it transfer or fail?
        """
        return

    def chk_on_startup(self):
        """
         Check to see if any pkgs are marked as TRANSFERRING
        """
        results = self.db_pkg.select_by_status("TRANSFERRING")

        for result in results:
            pid = result["pid"]
            pkg_id = result["pkg_id"]
            start_time = result["xfr_start_time"]

            if not self.is_running(pid, start_time):
                self.handle_missing_process(result)
                return

            xfr_obj = XfrProcess(
                pid=pid, pkg_id=pkg_id,
                start_time=start_time
            )
            self.active_xfr.add(xfr_obj)





