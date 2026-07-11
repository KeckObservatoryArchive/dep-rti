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


    def proc_obs(self, koaid):
        """
        Process a set of files with the KOAID.  The processing includes:
            (1) adding them to a tar package (packaging)
            (2) updating the database status to move the files through
                the pipeline.
        """

        # update the database to claim the file,  change status to PACKAGING
        self.db_obs.update_status_by_koaid(koaid, "PACKAGING")

        # get the filepath (can be overridden from default by filepath_replacement)
        # the CLI can update the filepath_replacement to allow alternative directory
        obs_path = self.db_obs.filepath_by_koaid(koaid)

        # get tarfile
        tarfile, pkg_id = self.ctx.tar_fun.get_current(koaid)

        # add file to tar
        num = self.ctx.tar_fun.add_obs(koaid, pkg_id, obs_path, tarfile)

        # if the
        if not num:
            return None

        # TODO ??? handle error if num == 0 ???

        # update the package id in the observations table
        num = self.db_obs.update_pkg_id(pkg_id, koaid)

        # update observation status to packaged
        num = self.db_obs.set_pkgd(koaid)

        # return the tar file (full path with filename) so it can be checked
        return tarfile


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

        # first pass through any tar files ending in .tmp
        tmp_tar_files = self.get_tmp_tarfiles()

        self.log.debug(f"tmp files: {tmp_tar_files}")

        for tmp_file in tmp_tar_files:
            self.log.info(f'Startup cleanup,  checking: {tmp_file}')
            if not self.ctx.tar_fun.is_valid(tmp_file):
                self.ctx.tar_fun.handle_invalid(tmp_file)

            # check if the file needs to be closed and marked for transfer
            _ = self.ctx.tar_fun.chk_finalize(tmp_file)

        # check any packages that have a status=OPEN and a .tar file (not .tar.tmp)
        self.chk_open_pkgs()

        # check any packages with .tmp filename and status=CLOSED
        self.chk_tmp_tarfiles()

        # Remove any remaining .tar.tmp files and reset related
        # observations to pending
        tmp_tar_files = self.get_tmp_tarfiles()
        for tmp_file in tmp_tar_files:

            # remove the temporary file if it is in the database or not
            self.log.info(f'Startup cleanup,  removing {tmp_file}')
            tmp_file.unlink(missing_ok=True)

            result = self.db_pkg.find_open_filename(tmp_file.name)
            if not result:
                continue

            # creation time on package remains the same providing the same
            # temporal limit.  Only will re-add the tar files for safety
            # inserted time on the obs will also remain the original time.
            self.db_obs.reset_by_pkg(result['pkg_id'])

        # reset all observations stuck PACKAGING to pending,
        self.db_obs.change_status('PACKAGING', 'PENDING')

        # reset all packages,  the name can be re-used if status is changed
        self.db_pkg.change_status('OPEN', 'IGNORE')

        return


    def get_tmp_tarfiles(self):
        """
        Find the temporary tar files in the tar file directory.

        return list(<pathlib.Path>): the matching paths of tarfiles for
                                        instrument + level
        """
        inst_prefixes = self.cfg[self.ctx.inst]['inst_prefixes']
        tmp_tar_files = []

        for prefix in inst_prefixes:
            glob_pattern = f"{prefix}.*{self.ctx.lev_str}.tar.tmp"
            tmp_tar_files.extend(self.ctx.tar_path.glob(glob_pattern))

        return tmp_tar_files


    def chk_open_pkgs(self):
        """
        Check for OPEN packages with .tar means it needs to be transferred,
        process was quit before database status update.
        """
        results = self.db_pkg.select_by_status(
            'OPEN', add_str=" AND TRIM(filename) NOT LIKE '%%.tmp'"
        )

        for result in results:
            pkg_id = result['pkg_id']
            filename = result['filename']
            filepath = result['filepath']

            # update the status of both the pkg and observations
            if Path(f"{filepath}/{filename}").is_file():
                self.db_pkg.update_status(pkg_id, 'CLOSED')
                self.db_obs.update_status_by_pkg(pkg_id, "PACKAGED")

        # check for open packages with .tmp.tar,  these will be re-built
        results = self.db_pkg.select_by_status('OPEN')
        for result in results:
            pkg_id = result['pkg_id']
            self.db_pkg.update_status(pkg_id, 'IGNORE')
            self.db_obs.update_status_by_pkg(pkg_id, "PENDING")


    def chk_tmp_tarfiles(self):
        """
        Check for .tar.tmp file with a status of CLOSED

            if package.tar.tmp exists and DB status = CLOSED
            mv package.tar.tmp to package.tar
        """
        results = self.db_pkg.select_by_status(
            'CLOSED', add_str="AND TRIM(filename) LIKE '%%.tmp'"
        )

        for result in results:
            pkg_id = result['pkg_id']
            filename = result['filename']

            if Path(filename).is_file():
                tmp_tar = Path(filename)
                self.ctx.tar_fun.close_file(pkg_id, tmp_tar)



