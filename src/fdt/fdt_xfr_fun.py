import signal
import subprocess

class FdtXfrFun:
    def __init__(self, ctx):
        self.cfg = ctx.cfg
        self.log = ctx.log

        self.db_pkg = ctx.db_pkg
        self.pkg_watch_period = self.cfg['pkg_watch_period']

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


    # def run(self):
    #     # get pkg_to_transfer
    #     """
    #     select * from pkg_observations where status="CLOSED" AND source_deleted=0
    #     AND instrument=%s and level=%s
    #     """
    #
    #     open_procs = set()
    #     while not self.stop_requested:
    #
    #         open_procs = self.chk_open_procs(open_procs)
    #         sleep(self.pkg_watch_period)
    #
    #         # TODO need to implement still
    #         get_ready_pkgs = self.db_pkg.ready_to_tansfer()
    #
    #         if not get_ready_pkgs:
    #             continue
    #
    #         for pkg in get_ready_pkgs:
    #             proc = self.transfer(pkg)
    #             if not proc:
    #                 continue
    #             open_procs.add(proc)
    #
    #     return

    def transfer(self, pkg_id):
        """
        spawn a process to do the transfer

        something needs to wait for it to complete to update the status and metrics
        """
        cmd = ["transfer", pkg_id]
        # detach from parent - start_new_session=True
        proc = subprocess.Popen(cmd, start_new_session=True)

        return proc

    def chk_open_procs(self, open_procs):
        """
        Check spawned processes to see if they are complete

        """
        still_open = set()
        for proc in open_procs:
            result = proc.poll()
            if not result:
                still_open.add(proc)
                continue

            # Process completed
            if result == 0:
                print("Tar completed successfully")
            else:
                print(f"Tar failed with exit code {result}")

        return still_open


"""
import subprocess
import time

# Start tar in the background
proc = subprocess.Popen([
    "tar",
    "-cf",
    "/tmp/test.tar",
    "/data/files"
])

# Continue doing other work
while True:
    print("Doing other work...")

    # Check if tar finished
    result = proc.poll()

    if result is not None:
        # Process completed
        if result == 0:
            print("Tar completed successfully")
        else:
            print(f"Tar failed with exit code {result}")
        break

    time.sleep(5)
"""

