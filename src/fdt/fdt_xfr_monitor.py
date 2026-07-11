"""
The monitor of the package table to start transfers
"""

from time import sleep


class FdtPkgMonitor(object):
    def __init__(self, ctx):
        self.log = ctx.log
        self.xfr_fun = ctx.xfr_functions


    def run(self):
        # get pkg_to_transfer
        """
        select * from pkg_observations where status="CLOSED" AND source_deleted=0
        AND instrument=%s and level=%s
        """

        open_procs = set()
        while not self.stop_requested:

            open_procs = self.chk_open_procs(open_procs)
            sleep(self.pkg_watch_period)

            # TODO need to implement still
            get_ready_pkgs = self.db_pkg.ready_to_tansfer()

            if not get_ready_pkgs:
                continue

            for pkg in get_ready_pkgs:
                proc = self.transfer(pkg)
                if not proc:
                    continue
                open_procs.add(proc)

        return