"""
FDT Package Processing class.
"""
from enum import member
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

        # update the package id in the observations table
        num_pkg_id = self.db_obs.update_pkg_id(pkg_id, koaid)

        # update observation status to packaged
        num_packaged = self.db_obs.set_pkgd(koaid)

        self.log.info(f"Updated pkg_id for {num_pkg_id} files,  and "
                      f"set {num_packaged} to PACKAGED.")

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
            self.log.info(f'Startup, cleanup,  checking: {tmp_file}')
            if not self.ctx.tar_fun.is_valid(tmp_file):
                self.ctx.tar_fun.handle_invalid(tmp_file)

            # check if the file needs to be closed and marked for transfer
            _ = self.ctx.tar_fun.need_close(tmp_file)

        # check any packages that have a status=OPEN
        open_tars = self.chk_open_pkgs()

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
            self.db_obs.reset_by_pkg(result['pkg_id'])

        # reset all observations stuck PACKAGING to pending,
        self.db_obs.change_status('PACKAGING', 'PENDING')

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

        Check for open packages with .tar.tmp,  these need to be rebuilt
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

        # if any observations stuck PACKAGING,  remove pkg
        good_tarpaths = []
        results = self.db_pkg.select_by_status('OPEN')
        for result in results:
            pkg_id = result['pkg_id']
            filename = result['filename']
            filepath = result['filepath']
            tmp_filepath = Path(f"{filepath}/{filename}")
            packaging = self.db_obs.select_by_status_pkg(pkg_id, 'PACKAGING')

            # if no packaging observations and file exists,  keep it
            if not packaging and tmp_filepath.is_file():
                good_tarpaths.append(tmp_filepath)
                self.log.debug(f"Adding to good tarpaths: {tmp_filepath}")
                continue

            self.log.debug(f"issue with tarpaths, removing: {tmp_filepath}")
            self.db_pkg.update_status(pkg_id, 'IGNORE')
            self.db_obs.update_status_by_pkg(pkg_id, "PENDING")
            self.ctx.tar_fun.remove_file(tmp_filepath)


        return good_tarpaths

        # TODO this may be extreme for large files
        # check for open packages with .tmp.tar,  these will be re-built
        # results = self.db_pkg.select_by_status('OPEN')
        # for result in results:
        #     pkg_id = result['pkg_id']
        #     self.db_pkg.update_status(pkg_id, 'IGNORE')
        #     self.db_obs.update_status_by_pkg(pkg_id, "PENDING")



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



