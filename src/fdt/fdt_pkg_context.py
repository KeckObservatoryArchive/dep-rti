"""
The context variables used by the module.
"""
from . import fdt_utils
from .fdt_database import DatabaseConnect
from .fdt_lock import FdtLock
from .fdt_database_fun import PkgTable, ObsTable
from .fdt_tar_fun import TarFun
from .fdt_pkg_fun import FdtPkgFun


class FdtPkgContext:
    def __init__(self, inst, lev, cfg_file, log, filepath=None, tar_path=None):

        self.log = log
        self.inst = inst
        self.lev = lev
        self.lev_str = f'lev{lev}'

        self.cfg = fdt_utils.read_config(cfg_file)
        fdt_utils.validate_cfg(self.cfg)

        self.dev = self.cfg['GENERAL']['dev']
        self.admin_email = self.cfg['GENERAL']['admin_email']

        self.proc_conn = DatabaseConnect(self.cfg["DATABASE"])
        self.proc_conn.connect()

        self.lock_conn = DatabaseConnect(self.cfg["DATABASE"])
        self.lock_conn.connect()
        self.lock = FdtLock(
            self.lock_conn,
            f"fdt_pkg_{inst}_{self.lev_str}_lock",
            log
        )

        self.db_pkg = PkgTable(inst, lev, self.proc_conn, log)
        self.db_obs = ObsTable(inst, lev, self.proc_conn, log)

        self.data_path = fdt_utils.define_data_path(self, filepath)
        self.tar_path = fdt_utils.define_tar_path(self, tar_path)

        self.tar_fun = TarFun(self)
        self.pkg_fun = FdtPkgFun(self)
        self.utils = fdt_utils