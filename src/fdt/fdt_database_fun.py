"""
The Database Functions
"""


class FdtDatabaseFun:

    def __init__(self, conn, log):
        self.conn = conn
        self.log = log

    # used to define the query type
    last_row_id = 0
    fetch_one = 1
    fetch_all = 2

    def _exec_q(self, query, params=None, qtype=None):
        """
        Execute the database query.

        query <str> -- The SQL query.
        params <list> -- The parameters to pass to the SQL query.
        qtype <int> -- The SQL query type.
        """
        self.conn.ensure_connected()

        try:
            # closes cursor on return
            with self.conn.db.cursor() as cur:
                cur.execute(query, params or ())

                if qtype == self.fetch_all:
                    return cur.fetchall()

                if qtype == self.fetch_one:
                    return cur.fetchone()

                if qtype == self.last_row_id:
                    return cur.lastrowid

                return cur.rowcount

        except Exception as err:
            self.log.exception("DB query failed")
            raise Exception(f"DB query failed: {err}")


class PkgTable(FdtDatabaseFun):

    def __init__(self, inst, lev, conn, log):
        """
        Object to handle the FDT database 'fdt_packages' table.
        """
        super().__init__(conn, log)
        self.conn = conn
        self.log = log
        self.lev = lev
        self.inst = inst

    # --------------------------
    # ---- SELECT Functions ----
    # --------------------------
    def select_by_status(self, status, add_str=None):
        """
        Get the package ID and filename by package status.

        status <str> -- The package status.

        return <list><dict> -- The package ID and filename of matching packages.
        """
        query = (
            "SELECT pkg_id, filename, filepath, xfr_pid,  xfr_start_time"
            " FROM fdt_packages "
            " WHERE STATUS=%s AND instrument=%s AND level=%s"
        )

        if add_str:
            query += add_str

        query += " ORDER BY creation_time"

        params = (status, self.inst, self.lev,)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def find_open_tar(self):
        """
        Get OPEN packages by filename.
        """
        query = (
            "SELECT pkg_id, status, filepath, filename"
            " FROM fdt_packages"
            " WHERE status IN ('OPEN', 'CLOSE_REQUESTED')"
        )

        params = ()

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def find_open_filename(self, filename):
        """
        Get OPEN packages by filename.
        """
        query = (
            "SELECT pkg_id, status, filesize_mb, creation_time"
            " FROM fdt_packages"
            " WHERE filename=%s AND STATUS='OPEN'"
        )

        params = (filename,)

        results = self._exec_q(query, params=params, qtype=self.fetch_one)

        return results

    def find_filename(self, filename):
        """
        Get packages by filename.
        """
        query = (
            "SELECT pkg_id, status, filesize_mb, creation_time"
            " FROM fdt_packages"
            " WHERE filename=%s"
        )

        params = (filename,)

        results = self._exec_q(query, params=params, qtype=self.fetch_one)

        return results

    def ready_to_transfer(self):
        """
        Find packages that are ready to transfer.
        """
        query = (
            "SELECT pkg_id, filepath, filename "
            " FROM fdt_packages "
            " WHERE status='CLOSED' "
            " AND source_deleted=0 AND instrument=%s and level=%s"
        )

        params = (self.inst, self.lev)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def expired_transfers(self, timeout):
        """
        Find packages with transfers that are expired.

        timeout <int> -- The number of minutes to wait for transfers.
        """
        query = (
            "SELECT pkg_id FROM fdt_packages "
            " WHERE status = 'TRANSFERRING' AND instrument=%s and level=%s "
            " AND xfr_start_time < (NOW() - INTERVAL %s MINUTE) "
        )

        params = (self.inst, self.lev, timeout)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def chk_for_errors(self):
        """
        Used to check for any errors in the packages table.  Stuck packages
        staying as TRANFERRING for >120 minutes are considered to be in error
        """
        query = (
            "SELECT pkg_id, status, error_message "
            " FROM fdt_packages "
            " WHERE instrument=%s AND level=%s "
            " AND (status='ERROR' AND error_reported IS NULL)"
            " OR (status='TRANSFERRING' "
            "     AND xfr_start_time < DATE_SUB(NOW(), INTERVAL 120 MINUTE))"
        )

        params = (self.inst, self.lev)

        results = self._exec_q(query, params=params, qtype=self.fetch_all) or []

        return results

    # --------------------------
    # ---- UPDATE Functions ----
    # --------------------------
    def update_status(self, pkg_id, status):
        """
        Update the status of the package.

        pkg_id <int> -- The package database ID.
        status <str> -- The package status.

        """
        query = (
            "UPDATE fdt_packages "
            " SET status = %s, error_message=NULL "
            " WHERE pkg_id = %s"
        )

        params = (status, pkg_id)

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(f"updated pkg id {pkg_id} to {status}.")

    def change_status(self, status_old, status_new, add_str=None):
        """
        Change the status of the package.

        status_old <str> -- The old status of the package.
        status_new <str> -- The new status of the package.

        return <int> -- number of packages updated.
        """
        query = (
            "UPDATE fdt_packages"
            " SET status = %s, error_message=NULL "
            " WHERE status = %s AND instrument=%s AND level=%s"
        )

        params = (status_new, status_old, self.inst, self.lev)

        if add_str:
            query += add_str

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(
                f"changed {num} packages from {status_old} to {status_new}."
            )

        return num

    def add_new(self, filename, filepath):
        """
        Add a new package or reopen an ignored package.

        return <int> -- package database ID.
        """
        # Check for an existing ignored package (set that way on starup_clean)
        query = (
            "SELECT pkg_id FROM fdt_packages "
            " WHERE filename=%s AND level=%s AND status='IGNORE'"
        )
        params = (filename, self.lev)

        result = self._exec_q(query, params=params, qtype=self.fetch_one)

        # reuse packages names with record but not transferred
        if result:
            pkg_id = result["pkg_id"]

            query = (
                "UPDATE fdt_packages "
                " SET status='OPEN', filesize_mb=0, source_deleted=0, "
                "     creation_time=NOW(), closed_time=NULL, "
                "     xfr_start_time=NULL, xfr_end_time=NULL "
                " WHERE pkg_id=%s"
            )

            self._exec_q(query, params=(pkg_id,))

            return pkg_id

        # Create a new package
        query = (
            "INSERT INTO fdt_packages (filename, filepath, level, "
            "                          instrument, status)"
            " VALUES (%s, %s, %s, %s, 'OPEN')"
        )

        params = (filename, filepath, self.lev, self.inst)

        return self._exec_q(query, params=params, qtype=self.last_row_id)

    def update_size(self, pkg_id, size):
        """
        Update the size of the package.
        """

        query = (
            "UPDATE fdt_packages "
            " SET filesize_mb = %s "
            " WHERE pkg_id = %s"
        )

        params = (size, pkg_id)

        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning(
                f'could not update package size for pkg_id: {pkg_id}.'
            )

        return num

    def closing_time(self, pkg_id):
        """
        Close the package.
        """
        query = (
            "UPDATE fdt_packages"
            " SET status='CLOSED', closed_time = NOW(), error_message=NULL "
            " WHERE pkg_id=%s AND status != 'CLOSED'"
        )
        params = (pkg_id,)

        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning(f"Package is already closed, pkg_id: {pkg_id}.")

        return num

    def update_filename(self, pkg_id, filename):
        """
        Update the filename of the package.

        pkg_id <int> -- The package database ID.
        filename <str> -- The new filename.

        """
        query = (
            "UPDATE fdt_packages"
            " SET filename=%s "
            " WHERE pkg_id=%s"
        )

        params = (filename, pkg_id,)
        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning("Error updating package filename.")

        return num

    def update_transferred(self, pkg_id, xfr_end_time):
        """
        Update the status of the package.

        status <str> -- The package status.
        pkg_id <int> -- The package database ID.
        """
        query = (
            "UPDATE fdt_packages "
            " SET status = 'TRANSFERRED', xfr_end_time=%s, error_message=NULL "
            " WHERE pkg_id = %s"
        )

        params = (xfr_end_time, pkg_id, )

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(f"updated pkg id {pkg_id} to TRANSFERRED.")

    def update_error(self, pkg_id, status, err_msg):
        """
        Update the status of the package.

        status <str> -- The package status.
        pkg_id <int> -- The package database ID.
        """
        query = (
            "UPDATE fdt_packages "
            " SET status = %s, error_message=%s "
            " WHERE pkg_id = %s"
        )

        params = (status, err_msg, pkg_id, )

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(f"updated pkg id {pkg_id} to ERROR.")

    def update_pid(self, pkg_id, pid, xfr_start_time):
        """
        Update the package transfer process id (xfr_pid) and set the status
        to transferring.

        pkg_id <int> -- The package database ID.
        pid <int> -- transfer process id.
        xfr_start_time <datetime> -- process creation time.

        """
        query = (
            "UPDATE fdt_packages "
            " SET xfr_pid=%s, xfr_start_time=%s, "
            "     status='TRANSFERRING', error_message=NULL  "
            " WHERE pkg_id=%s"
        )

        params = (pid, xfr_start_time, pkg_id)
        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning("Error updating package pid.")

        return num

    def reset_status_by_daterange(self, start, end, add=None):
        """
        Used to re-transfer a package,  generally in ERROR state.

        start <str> -- The start date packages to re-transfer.
        end <str> -- The end date packages to re-transfer.
        """
        query = (
            "UPDATE fdt_packages "
            " SET status='CLOSED',  xfr_start_time=NULL, xfr_end_time=NULL,"
            "     error_message=NULL "
            " WHERE closed_time "
            " BETWEEN %s AND %s AND level=%s AND instrument=%s"
        )

        params = (start, end, self.lev, self.inst)

        if add:
            query += add

        num = self._exec_q(query, params=params)
        if num == 0:
            self.log.error(
                f"could not update package status for date range "
                f"{start} to {end}."
            )

        return num


class ObsTable(FdtDatabaseFun):

    def __init__(self, inst, lev, conn, log):
        """
        Object to handle the FDT database 'fdt_observations' table.
        """
        super().__init__(conn, log)
        self.conn = conn
        self.log = log
        self.lev = lev
        self.inst = inst

    # --------------------------
    # ---- SELECT Functions ----
    # --------------------------
    def select_by_status(self, status):
        """
        Get the filepaths by status.

        status <str> -- The package status.
        """
        query = (
            "SELECT koaid, filepath, koaid, pkg_id "
            " FROM fdt_observations "
            " WHERE STATUS = %s AND level=%s AND instrument=%s"
            " ORDER BY inserted_time"
        )
        params = (status, self.lev, self.inst)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def update_status_by_koaid(self, koaid, status):
        """
        Update the status of the observation.

        koaid <str> -- The koaid.
        status <str> -- The package observation status.
        """
        query = (
            "UPDATE fdt_observations "
            " SET STATUS = %s "
            " WHERE koaid = %s"
        )

        num = self._exec_q(query, params=(status, koaid, ))
        if num == 0:
            self.log.debug(f"could not update status = {status} for {koaid}.")

        return num

    def select_by_status_pkg(self, pkg_id, status):
        """
        Update the package observation status.

        pkg_id <int> -- The package database ID.
        status <str> -- The package observation status.

        return <int> -- number of packages updated.

        """
        query = (
            "SELECT obsid, koaid, status "
            " FROM fdt_observations "
            " WHERE pkg_id = %s AND status=%s "
            " ORDER BY last_mod DESC "
        )
        params = (pkg_id, status, )

        num = self._exec_q(query, params=params)
        if num == 0:
            self.log.debug(
                f"No observations with pkg_id {pkg_id} and status {status}. "
            )

        return num

    def filepath_by_koaid(self, koaid):
        """
        Get the filepaths by koaid.

        koaid <str> -- The koaid.

        return <str> -- The filepath.
        """
        query = (
            "SELECT filepath, filepath_replacement "
            " FROM fdt_observations "
            " WHERE koaid=%s AND level=%s"
        )
        params = (koaid, self.lev)

        results = self._exec_q(query, params=params, qtype=self.fetch_one)

        if results['filepath_replacement']:
            return results['filepath_replacement']

        return results['filepath']

    def search_koaids(self, koaids):
        """
        Find observations by a list of koaids.

        koaids <list><str> -- The list of koaids.
        """

        koaid_str = ", ".join(["%s"] * len(koaids))

        query = f"""
            SELECT koaid, status, pkg_id
            FROM fdt_observations
            WHERE koaid IN ({koaid_str})
        """

        results = self._exec_q(query, params=koaids, qtype=self.fetch_all)

        return results

    def koaids_in_pkg(self, pkg_id):
        """
        Find all koaids (observations) with a given pkg_id.
        """
        query = (
            "select koaid from fdt_observations where pkg_id=%s"
        )
        params = (pkg_id,)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def chk_for_errors(self):
        """
        Checks for both errors and stalled observations.
        """
        query = (
            "SELECT obsid, koaid, status, error_message"
            " FROM fdt_observations "
            " WHERE instrument=%s AND level=%s "
            " AND ("
            "   (status='ERROR' AND error_reported IS NULL) "
            "   OR (status IN ('PENDING','PACKAGING','TRANSFERRING') "
            "   AND last_mod < DATE_SUB(NOW(), INTERVAL 120 MINUTE))"
            " )"
        )

        params = (self.inst, self.lev)

        results = self._exec_q(query, params=params, qtype=self.fetch_all) or []

        return results


    # --------------------------
    # ---- UPDATE Functions ----
    # --------------------------
    def update_status_by_pkg(self, pkg_id, status):
        """
        Update the package observation status.

        status <str> -- The package observation status.
        pkg_id <int> -- The package database ID.

        return <int> -- number of packages updated.

        """
        query = (
            "UPDATE fdt_observations "
            "SET STATUS = %s "
            "WHERE pkg_id = %s"
        )

        num = self._exec_q(query, params=(status, pkg_id,))
        if num > 0:
            self.log.debug(
                f"updated {num} observations in pkg_id: {pkg_id} to {status}."
            )

        return num

    def set_unknown(self, pkg_id, match_status):
        """
        Update the package observation status.

        match_status <str> -- The observation status to change from.
        pkg_id <int> -- The package database ID.

        return <int> -- number of packages updated.

        """
        query = (
            "UPDATE fdt_observations "
            " SET STATUS='UNKNOWN' "
            " WHERE pkg_id = %s and STATUS = %s"
        )

        params = (pkg_id, match_status)
        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(
                f"updated {num} observations in pkg_id: {pkg_id} to UNKNOWN."
            )

        return num

    def reset_by_pkg(self, pkg_id, err_msg):
        """
        Update the package observation status,  update the package to IGNORE.

        status <str> -- The package observation status.
        pkg_id <int> -- The package database ID.

        return <int> -- number of packages updated.
        """
        query = (
            "UPDATE fdt_packages "
            " SET STATUS = 'IGNORE', error_message=%s "
            " WHERE pkg_id = %s"
        )
        num = self._exec_q(query, params=(err_msg, pkg_id,))
        if num == 0:
            self.log.warning(f'Reset package,  pkg_id {pkg_id} not found.')

        query = (
            "UPDATE fdt_observations "
            " SET STATUS = 'PENDING', "
            "     PKG_START_TIME = NULL, PKG_END_TIME = NULL, PKG_ID = NULL "
            " WHERE pkg_id = %s"
        )

        num = self._exec_q(query, params=(pkg_id,))
        if num > 0:
            self.log.debug(
                f"reset {num} observations in package {pkg_id}."
            )

        return num

    def update_pkg_id(self, pkg_id, koaid):
        """
        Update a set of observations by pkg_id.

        pkg_id <int> -- The package database ID.
        koaid <str> -- The koaid.
        """
        query = (
            "UPDATE fdt_observations "
            " SET pkg_id=%s "
            " WHERE koaid=%s"
        )

        num = self._exec_q(query, params=(pkg_id,koaid,))

        if num == 0:
            self.log.debug(
                f"could not update pkg_id = {pkg_id} for '{koaid}'."
            )

        return num

    def set_pkgd(self, koaid):
        """
        Update an observation status to packaged by the koaid.

        koaid <str> -- The koaid.
        """
        query = (
            "UPDATE fdt_observations "
            " SET status = 'PACKAGED', filepath_replacement = NULL "
            " WHERE koaid = %s"
        )

        num = self._exec_q(query, params=(koaid,))
        if num == 0:
            self.log.debug(f"could not update status = PACKAGED for {koaid}.")

        return num

    def repackage_by_daterange(self, start, end, add=None):
        """
        Used by the CLI to re-package observations.

        start <str> -- The start date of observations to reprocess.
        end <str> -- The end date (inclusive) of observations to reprocess.
        add <str> -- add to the end of the query.
        """
        query = (
            "UPDATE fdt_observations "
            " SET status='PENDING', pkg_id=NULL "
            " WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(koaid, '.', 2), '.', -1) "
            " BETWEEN %s AND %s AND level=%s  AND instrument=%s"
        )
        params = (start, end, self.lev, self.inst)

        if add:
            query += add

        num = self._exec_q(query, params=params)
        if num == 0:
            self.log.error(
                f"could not update observation status for date range "
                f"{start} to {end}."
            )

        return num

    def change_status(self, status_old, status_new):
        """
        Change the status of the observations.  Matches on old status.

        status_old <str> -- The old status.
        status_new <str> -- The new status.
        """
        query = (
            "UPDATE fdt_observations "
            " SET status=%s "
            " WHERE status=%s AND level=%s AND instrument=%s "
        )
        params = (status_new, status_old, self.lev, self.inst)

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(
                f"Changed {num} observations from {status_old} to {status_new} ."
            )

        return num

    def update_start_time(self, koaid):
        """
        Update the package start time of the observations by koaid.

        koaid <str> -- The koaid.
        """
        query = (
            "UPDATE fdt_observations "
            " SET pkg_start_time=NOW() "
            " WHERE koaid=%s"
        )

        num = self._exec_q(query, params=(koaid, ))
        if num == 0:
            self.log.warning(
                f"Could not update {num} pkg_start_time for {koaid}."
            )

        return num

    def update_end_time(self, koaid):
        query = (
            "UPDATE fdt_observations "
            " SET pkg_end_time=NOW() "
            " WHERE koaid=%s"
        )

        num = self._exec_q(query, params=(koaid, ))
        if num == 0:
            self.log.warning(
                f"Could not update {num} pkg_end_time for {koaid}."
            )

        return num

    def set_replacement_path(self, koaid, filepath):
        """
        Add a second path for re-processing via a path.

        The status becomes PENDING,  thus triggering a package / transfer
        """
        query = (
            "UPDATE fdt_observations "
            " SET pkg_start_time=NULL, pkg_end_time=NULL, status='PENDING',"
            "     filepath_replacement=%s"
            " WHERE koaid=%s"
        )
        params = (filepath, koaid)

        num = self._exec_q(query, params=params)
        if num == 0:
            self.log.warning(
                f"Could not update filepath_replacement for {koaid}."
            )

        return num

    def insert_obs(self, koaid, filepath, status):
        """
        Insert a new row,
        """
        query = (
            "INSERT INTO fdt_observations "
            "  (koaid, filepath, status, level, instrument) "
            " VALUES (%s, %s, %s, %s, %s)"
        )

        params = (koaid, filepath, status, self.lev, self.inst)

        return self._exec_q(query, params=params, qtype=self.last_row_id)


    def update_transferred(self, pkg_id, xfr_end_time):
        """
        Update the status of the package.

        status <str> -- The package status.
        pkg_id <int> -- The package database ID.
        """
        query = ("UPDATE fdt_observations "
                 " SET STATUS = 'TRANSFERRED', xfr_end_time=%s "
                 " WHERE pkg_id = %s")

        params = (pkg_id, xfr_end_time)

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(f"updated pkg id {pkg_id} to TRANSFERRED.")





