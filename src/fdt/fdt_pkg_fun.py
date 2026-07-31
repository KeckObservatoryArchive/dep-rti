"""
FDT Package Processing class.
"""
from pathlib import Path


class FdtPkgFun:
    """
    The primary FDT packaging class.
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.log = ctx.log
        self.cfg = ctx.cfg

        self.db_obs = ctx.db_obs
        self.db_pkg = ctx.db_pkg

        # general
        self.admin_email = self.ctx.cfg['GENERAL']['admin_email']

    def proc_obs(self, koaid):
        """
        Process a set of files with the KOAID.  The processing includes:
            (1) adding them to a tar package (packaging)
            (2) updating the database status to move the files through
                the pipeline.
        """

        # update the database to claim the file,  change status to PACKAGING
        self.db_obs.update_status_by_koaid(koaid, "PACKAGING")

        # get the filepath (can be overridden by CLI - filepath_replacement)
        obs_path = self.db_obs.filepath_by_koaid(koaid)

        # get tarfile
        tarfile, pkg_id = self.ctx.tar_fun.get_current(koaid)

        # add file to tar
        num = self.ctx.tar_fun.add_obs(koaid, pkg_id, obs_path, tarfile)

        # if the
        if not num:
            return

        # update the package id in the observations table
        num_pkg_id = self.db_obs.update_pkg_id(pkg_id, koaid)

        # update observation status to packaged
        num_packaged = self.db_obs.set_pkgd(koaid)

        self.log.info(f"Updated {koaid} status to PACKAGED and pkg_id "
                      f"{num_pkg_id},  total updated {num_packaged}.")

    def chk_for_new_files(self):
        """
        Find all observations with the status = PENDING.
        """

        observations = self.db_obs.select_by_status('PENDING')

        return observations

    def startup_clean(self):
        """
        Cleanup in case the packages (tar files) and database were left in
        an invalid state on the last exit of the monitor

        """

        # check any packages that have a status=OPEN
        open_tars = self.chk_open_pkgs()
        open_tars = self.chk_finalize_tars(tarfile_set=open_tars)
        self.log.info(f"open tar files: {open_tars}")

        # first pass through any tar files ending in .tmp
        tmp_tar_files = self.get_tmp_tarfiles()

        self.log.debug(f"tmp files: {tmp_tar_files}")

        for tmp_file in tmp_tar_files:
            self.log.info(f'Startup, cleanup,  checking: {tmp_file}')
            if not self.ctx.tar_fun.is_valid(tmp_file):
                self.ctx.tar_fun.handle_invalid(tmp_file)

            # check if the file needs to be closed and marked for transfer
            _ = self.ctx.tar_fun.need_close(tmp_file)

        # check any packages with .tmp filename and status=CLOSED
        self.chk_tmp_tarfiles()

        # Remove any remaining .tar.tmp files and reset related
        # observations to pending
        tmp_tar_files = self.get_tmp_tarfiles()
        for tmp_file in tmp_tar_files:
            # skip open files deemed to be good
            if tmp_file in open_tars:
                continue

            # remove the temporary file if it is in the database or not
            self.log.info(f'Startup, cleanup,  removing {tmp_file}')

            self.ctx.tar_fun.remove_file(tmp_file)

            result = self.db_pkg.find_open_filename(tmp_file.name)

            # continue if the pkg filename is not in the database
            if not result:
                continue

            # creation time on package remains the same providing the same
            # temporal limit.  Only will re-add the tar files for safety
            # inserted time on the obs will also remain the original time.
            # set the package to IGNORE
            err_msg = f"package abandoned and recreated"
            self.db_obs.reset_by_pkg(result['pkg_id'], err_msg)

        # reset all observations stuck PACKAGING to pending,
        self.db_obs.change_status('PACKAGING', 'PENDING')

        return

    def chk_finalize_tars(self, tarfile_set=None):
        """
        Only one tarfile within the instrument and level should only be open
        at one time.


        tarfile_set <set>: the tarfile PATHs to check
        """
        if tarfile_set:
            # check the set
            open_tar = set()
            for open_file in tarfile_set:
                tar_path = Path(open_file)

                if not self.ctx.tar_fun.need_close(tar_path):
                    open_tar.add(tar_path)

            return open_tar

        results = self.db_pkg.find_open_tar()
        for result in results:
            filename = result['filename']
            filepath = result['filepath']
            tar_path = Path(f"{filepath}/{filename}")
            self.ctx.tar_fun.need_close(tar_path)

        return None

    def get_tmp_tarfiles(self):
        """
        Find the temporary tar files in the tar file directory.

        return list(<pathlib.Path>): the matching paths of tarfiles for
                                        instrument + level
        """
        tmp_tar_files = []

        prefix = f"{self.ctx.inst}_"
        glob_pattern = f"{prefix}*{self.ctx.lev_str}.tar.tmp"
        tmp_tar_files.extend(self.ctx.tar_path.glob(glob_pattern))

        return tmp_tar_files

    def chk_open_pkgs(self):
        """
        Check for OPEN packages with .tar means it needs to be transferred,
        process was quit before database status update.

        Check for open packages with .tar.tmp,  these need to be rebuilt
        """

        # open but .tar ending (meaning was finalized)
        results = self.db_pkg.select_by_status(
            'OPEN', add_str=" AND TRIM(filename) NOT LIKE '%%.tmp'"
        )

        for result in results:
            pkg_id = result['pkg_id']
            filename = result['filename']
            filepath = result['filepath']

            # file exists, update the status of both the pkg and observations
            if Path(f"{filepath}/{filename}").is_file():
                self.db_pkg.update_status(pkg_id, 'CLOSED')
                self.db_obs.update_status_by_pkg(pkg_id, "PACKAGED")
            # no file,  set observations to be re-packaged
            else:
                self.db_pkg.update_error(pkg_id, 'ERROR', 'FILE_MISSING')
                self.log.warning(f'set pkg_id {pkg_id} missing.')
                self.db_obs.update_status_by_pkg(pkg_id, "PENDING")

        # all other OPEN packages
        good_tarpaths = set()
        results = self.db_pkg.find_open_tar()
        for result in results:
            pkg_id = result['pkg_id']
            filename = result['filename']
            filepath = result['filepath']
            tmp_filepath = Path(f"{filepath}/{filename}")
            packaging = self.db_obs.select_by_status_pkg(pkg_id, 'PACKAGING')

            # if no packaging observations and file exists,  keep it
            if not packaging and tmp_filepath.is_file():
                good_tarpaths.add(tmp_filepath)
                self.log.debug(f"Adding to good tarpaths: {tmp_filepath}")
                continue

            # remove the tarfile (if it exists or not)
            self.log.debug(f"issue with tarpaths, removing: {tmp_filepath}")

            # set the package to IGNORE,  a .tmp file so not ERROR
            self.db_pkg.update_status(pkg_id, 'IGNORE')

            # reset the observations so they are re-pacakged.
            self.db_obs.update_status_by_pkg(pkg_id, "PENDING")
            self.ctx.tar_fun.remove_file(tmp_filepath)

        return good_tarpaths

    def chk_tmp_tarfiles(self):
        """
        Check for .tar.tmp file with a status of CLOSED

        Close all files that were left in a .tar.tmp state.
        """
        results = self.db_pkg.select_by_status(
            'CLOSED', add_str="AND TRIM(filename) LIKE '%%.tmp'"
        )

        for result in results:
            pkg_id = result['pkg_id']
            full_path = Path(f"{result['filepath']}/{result['filename']}")

            if full_path.is_file():
                self.ctx.tar_fun.close_file(pkg_id, full_path)
            else:
                self.db_pkg.update_status(pkg_id, 'IGNORE')
