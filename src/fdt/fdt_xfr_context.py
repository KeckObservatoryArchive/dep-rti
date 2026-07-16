"""
The context variables used by the module.
"""
from . import fdt_utils
from .fdt_database import DatabaseConnect
from .fdt_lock import FdtLock
from .fdt_database_fun import PkgTable, ObsTable
from .fdt_xfr_fun import FdtXfrFun
from .fdt_tar_fun import TarFun


class FdtXfrContext:
    def __init__(self, inst, lev, cfg_file, log):

        self.log = log
        self.inst = inst
        self.lev = lev
        self.lev_str = f'lev{lev}'

        self.cfg = fdt_utils.read_config(cfg_file)
        fdt_utils.validate_cfg(self.cfg)

        self.proc_conn = DatabaseConnect(self.cfg["DATABASE"])
        self.proc_conn.connect()

        self.lock_conn = DatabaseConnect(self.cfg["DATABASE"])
        self.lock_conn.connect()
        self.lock = FdtLock(
            self.lock_conn,
            f"fdt_watch_{inst}_{self.lev_str}_lock",
            log
        )

        self.db_pkg = PkgTable(inst, lev, self.proc_conn, log)
        self.db_obs = ObsTable(inst, lev, self.proc_conn, log)

        self.tar_fun = TarFun(self)
        self.xfr_fun = FdtXfrFun(self)