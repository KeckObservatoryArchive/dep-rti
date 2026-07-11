"""
The Database Functions
"""
from ast import literal_eval


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

        except Exception:
            self.log.exception("DB query failed")
            raise


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


    def update_status(self, pkg_id, status):
        """
        Update the status of the package.

        status <str> -- The package status.
        pkg_id <int> -- The package database ID.
        """
        query = (
            "UPDATE fdt_packages "
            " SET STATUS = %s "
            " WHERE pkg_id = %s"
        )

        params = (status, pkg_id)

        num = self._exec_q(query, params=params)
        if num > 0:
            self.log.debug(f"updated {num} package {pkg_id} to {status}.")


    def select_by_status(self, status, add_str=None):
        """
        Get the package ID and filename by package status.

        status <str> -- The package status.
        pkg_id <int> -- The package database ID.

        return <list><dict> -- The package ID and filename of matching packages.
        """
        query = (
            "SELECT pkg_id, filename, filepath "
            " FROM fdt_packages "
            " WHERE STATUS=%s AND instrument=%s AND level=%s"
        )

        if add_str:
            query += add_str

        query += " ORDER BY creation_time"

        params = (status, self.inst, self.lev,)

        results = self._exec_q(query, params=params, qtype=self.fetch_all)

        return results

    def change_status(self, status_old, status_new, add_str=None):
        """
        Change the status of the package.

        status_new <str> -- The new status of the package.
        status_old <str> -- The old status of the package.

        return <int> -- number of packages updated.
        """
        query = (
            "UPDATE fdt_packages"
            " SET status = %s "
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
                " SET status='OPEN', filesize_mb=0, koaid_count=0, "
                "     source_deleted=0, creation_time=NOW(), closed_time=NULL, "
                "     xfr_start_time=NULL, xfr_end_time=NULL "
                " WHERE pkg_id=%s"
            )

            self._exec_q(query, params=(pkg_id,))

            return pkg_id

        # Create a new package

        # get any packages with the same name,  increment run number
        query = (
            "SELECT COALESCE(MAX(run_number), 0) + 1 AS next_run "
            " FROM fdt_packages "
            " WHERE filename LIKE %s"
        )
        basename = filename.removesuffix(".tmp").removesuffix(".tar")
        params = f"{basename}%"

        result = self._exec_q(query, params=params, qtype=self.fetch_one)

        run_number = result["next_run"]

        query = (
            "INSERT INTO fdt_packages (filename, filepath, level, "
            "                          instrument, status, run_number)"
            " VALUES (%s, %s, %s, %s, 'OPEN', %s)"
        )

        params = (filename, filepath, self.lev, self.inst, run_number)

        return self._exec_q(query, params=params, qtype=self.last_row_id)

    def update_size(self, pkg_id, size, update_cnt=0):
        """
        Update the size of the package.
        """
        try:
            int(update_cnt)
        except ValueError:
            update_cnt = 0

        query = (
            "UPDATE fdt_packages "
            "SET filesize_mb = %s, koaid_count = koaid_count + %s "
            "WHERE pkg_id = %s"
        )

        params = (size, update_cnt, pkg_id)

        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning(f'could not update package size for pkg_id: {pkg_id}.')

        return num

    def get_size(self, pkg_id):
        """
        Update the size of the package.
        """
        query = (
            "SELECT filesize FROM fdt_packages WHERE pkg_id = %s"
        )

        params = (pkg_id, )

        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning(f'could not get the size of pkg_id: {pkg_id}.')

        return num


    def closing_time(self, pkg_id):
        query = (
            "UPDATE fdt_packages"
            " SET status='CLOSED', closed_time = NOW() "
            " WHERE pkg_id=%s AND status != 'CLOSED'"
        )
        params = (pkg_id,)

        num = self._exec_q(query, params=params)

        if num == 0:
            self.log.warning(f"Package is already closed, pkg_id: {pkg_id}.")

        return num

    def mark_prev_deleted(self, pkg_id):
        """
        Mark all other packages as deleted.  The mv from tar.tmp to .tar
        overwrites any packages with .tar
        """
        query = (
            "UPDATE fdt_packages p"
            " JOIN fdt_packages cur"
            " ON REPLACE(p.filename, '.tmp', '') = "
            "    REPLACE(cur.filename, '.tmp', '')"
            " SET p.source_deleted = 1 "
            " WHERE cur.pkg_id = %s"
            " AND p.pkg_id <> cur.pkg_id"
        )

        num = self._exec_q(query, params=(pkg_id, ))

        return num

    def find_by_id(self, pkg_id):

        query = (
            "SELECT filename, status, filesize_mb, creation_time"
            " FROM fdt_packages"
            " WHERE pkg_id=%s "
        )

        params = (pkg_id,)

        results = self._exec_q(query, params=params, qtype=self.fetch_one)

        return results

    # def find_by_filename(self, filename):
    def find_open_filename(self, filename):

        query = (
            "SELECT pkg_id, status, filesize_mb, creation_time"
            " FROM fdt_packages"
            " WHERE filename=%s AND STATUS='OPEN'"
        )

        params = (filename,)

        results = self._exec_q(query, params=params, qtype=self.fetch_one)

        return results

    def update_filename(self, pkg_id, filename):
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
                f"updated {num} observations in package "
                f"{pkg_id} to {status}."
            )

        return num

    def reset_by_pkg(self, pkg_id):
        """
        Update the package observation status.

        status <str> -- The package observation status.
        pkg_id <int> -- The package database ID.

        return <int> -- number of packages updated.

        """
        query = (
            "UPDATE fdt_observations "
            " SET STATUS = %s, PKG_START_TIME = NULL, PKG_END_TIME = NULL, "
            " PKG_ID = NULL "
            " WHERE pkg_id = %s"
        )

        num = self._exec_q(query, params=('PENDING', pkg_id,))
        if num > 0:
            self.log.debug(
                f"reset {num} observations in package {pkg_id}."
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


    def select_by_status(self, status):
        """
        Get the filepaths by status.

        status <str> -- The package status.
        """
        query = (
            "SELECT koaid, filepath, pkg_id "
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

    def update_pkg_id(self, pkg_id, koaid):

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
        query = (
            "UPDATE fdt_observations "
            " SET status = 'PACKAGED', filepath_replacement = NULL "
            " WHERE koaid = %s"
        )

        num = self._exec_q(query, params=(koaid,))
        if num == 0:
            self.log.debug(f"could not update status = PACKAGED for {koaid}.")

        return num


    def update_status_by_daterange(self, start, end, add=None):
        query = (
            "UPDATE fdt_observations"
            " SET status='PENDING',"
            " pkg_id=NULL"
            " WHERE utd BETWEEN %s AND %s AND level=%s AND instrument=%s "
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

