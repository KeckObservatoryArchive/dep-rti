"""
The FDT Processing TAR file functions.
"""
import os
import glob
import yaml
import tarfile
from pathlib import Path
from datetime import datetime, timedelta

class TarFun:

    def __init__(self, ctx):
        self.ctx = ctx
        self.log = ctx.log

        self.retry = False
        self.max_pkg_size = ctx.cfg[ctx.inst]['max_pkg_size']
        self.time_limit = timedelta(minutes=ctx.cfg['FDT_PKG']['pkg_timeout'])

    def add_obs(self, koaid, pkg_id, obs_path, tar_path):
        """
        Add all the files with a KOAID to a tar file

        tar_path <str>: full path of the tar file including file name
        """

        if not tar_path.is_file():
            self.log.warning(
                f'Tar full_path {tar_path} does not exist, '
                f'opening new file'
            )
            tar_path, pkg_id = self.handle_missing(pkg_id)

        try:
            obs_dir = Path(obs_path).parent
            with tarfile.open(tar_path, "a") as tar:
                for filename in glob.glob(f"{obs_dir}/{koaid}*"):
                    self.log.debug(f'Adding: {filename} to {tar_path}')
                    tar.add(filename, arcname=Path(filename).name)
        except Exception as err:
            self.log.error(f"Failed to add {koaid} to {tar_path}: {err}")
            return None

        try:
            tar_mb = self.get_file_size(tar_path)
        except FileNotFoundError:
            tar_path, pkg_id = self.handle_missing(pkg_id)
            return self.add_obs(koaid, pkg_id, obs_path, tar_path)

        self.log.debug(f"File size, {tar_path}: {tar_mb} MB")

        # this updates the file size and the KOAID obs_count by 1
        num_updated = self.ctx.db_pkg.update_size(pkg_id, tar_mb)

        return num_updated

    def add_cfg(self, pkg_id, tar_path):
        """
        Add the cfg file used by IPAC
        """
        cfg_path = tar_path.with_suffix(".cfg")
        success = self.create_cfg(pkg_id, cfg_path, tar_path)

        try:
            with tarfile.open(tar_path, "a") as tar:
                tar.add(cfg_path, arcname=Path(cfg_path).name)
        except Exception as err:
            self.log.error(f"Failed to add cfg to {pkg_id}: {err}")
            success = False
        finally:
            if cfg_path.exists():
                cfg_path.unlink()

        return success

    def create_cfg(self, pkg_id, cfg_path, tar_path):
        """
        Create the cfg file to accompany the tar file.  The file contains
        three sections,  instrument,  level,  and koaids.

        pkg_id <int>: the database id of the package (tarfile)
        cfg_path <Path>: the path to the cfg file
        tar_path <Path>: the path to the tar file
        """

        # find the koaids included in the tar file
        try:
            koaids = self.included_koaids(tar_path)
        except Exception as err:
            self.log.error(f"Failed to get koaids from {tar_path}: {err}")
            return False

        # confirm the database says the same
        db_koaids = {row["koaid"] for row in self.ctx.db_obs.koaids_in_pkg(pkg_id)}

        # log the difference if it exists
        if koaids != db_koaids:
            self.log.error(
                f"{pkg_id} KOAID mismatch. "
                f"Missing from DB: {koaids - db_koaids}, "
                f"Missing from tar: {db_koaids - koaids}"
            )

        cfg_data = {
            "instrument": self.ctx.inst,
            "ingesttype": self.ctx.lev_str,
            "filelist": sorted(koaids)
        }

        cfg_path.write_text(
            yaml.safe_dump(
                cfg_data, sort_keys=False, default_flow_style=False, indent=2
            )
        )

        return True

    def get_file_size(self, full_path):
        """
        Find the filesize of the tar package,  accepts either Path Object or
        a full_path string.

        full_path <str>: full path of the tar file including file name
        """

        try:
            # get the new size of the tar file,  update the database
            file_size = os.path.getsize(full_path)
            size_mb = round(file_size / (1024 ** 2), 2)
        except FileNotFoundError as err:
            raise
        except Exception as err:
            self.log.error(f"Failed to find file size: {err}")
            size_mb = -1

        return size_mb

    def is_valid(self, filepath):
        """
        Check if the tar file is valid.  This can be used
        after a restart of the process creating the tar file.

        filepath <Path> - the Path object of the tar file
        """
        try:
            # read full file not just headers
            with tarfile.open(filepath, "r") as tar:
                tar.getmembers()
            return True
        except tarfile.ReadError:
            return False
        except tarfile.TarError:
            return False
        except Exception as e:
            self.log.error(f"[FDT] Unexpected tar error: {e}")

            return False

    def handle_invalid(self, filename):
        """
        find the pkg_id in the database by filename

        update the observations status with pkg_id -> PENDING

        remove the tar file

        update the db_pkg.update_status = IGNORE

        """

        # find the pkg_id
        pkg_id = self.ctx.db_pkg.find_open_filename(filename)

        # update the observations to PENDING so they will be re-added
        num = self.ctx.db_obs.update_status_by_pkg(pkg_id, 'PENDING')
        self.log.info(f"updated {num} files with pkg_id {pkg_id} to PENDING.")

        # remove the file,  okay if it is already deleted for some reason
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
        except Exception as err:
            self.log.exception(f"Could not remove {filename}")
            raise Exception(f"Invalid tarfile: {err}")

        # update the package status to IGNORE
        _ = self.ctx.db_pkg.update_status(pkg_id, 'IGNORE')

    def need_close(self, filepath_obj):
        """
        Check if the tar needs to be closed.  If it meets the full criteria,
        finalize it by (1) updating the DB status (2)

        filepath_obj <Path> - the Path object of the tar file
        pkg_id <int> - the pkg_id in the database

        """
        result = self.ctx.db_pkg.find_filename(filepath_obj.name)

        if not result:
            self.log.error(f"Could not find {filepath_obj}")
            return False

        pkg_id = result['pkg_id']
        create_time = result['creation_time']
        size_mb = result['filesize_mb']

        # missing file handled elsewhere
        try:
            tar_mb = self.get_file_size(filepath_obj)
        except FileNotFoundError:
            return False

        if tar_mb > size_mb:
            self.ctx.db_pkg.update_size(pkg_id, tar_mb)
            size_mb = tar_mb

        now = datetime.now()

        # (1) CLI close, (2) check size limit, (3) check temporal limit
        broken_rules = [
            result['status'] == 'CLOSE_REQUESTED',
            size_mb >= self.max_pkg_size,
            now - create_time >= self.time_limit
        ]

        if any(broken_rules):
            self.close_file(pkg_id, filepath_obj)
            return True

        return False

    def close_file(self, pkg_id, tar_filepath):
        """
        Rename the file from .tar.tmp to .tar

        Update the name in the database.

        tar_filepath <Path> - the Path object of the tar file
        """
        self.log.info(f"Closing, pkg_id: {pkg_id}, filename {tar_filepath}")

        # move .tar.tmp -> .tar
        new_filepath = tar_filepath.with_suffix("")
        tar_filepath.replace(new_filepath)

        # update the database
        new_filename = tar_filepath.with_suffix("").name
        self.log.debug(f"New file name: {new_filepath}")
        num = self.ctx.db_pkg.update_filename(pkg_id, new_filename)

        # set the status of the pkg observations to PACKAGED
        num = self.ctx.db_obs.update_status_by_pkg(pkg_id, "PACKAGED")
        self.log.debug(f"Set packaged: {pkg_id} to PACKAGED")

        # update the close time for metrics
        self.ctx.db_pkg.closing_time(pkg_id)
        self.log.debug(f"Set closing time: {pkg_id}.")

        return num

    def get_current(self, koaid):

        """
        select pkg_id, filename from fdt_packages where status = OPEN

        if no currently packaging tar entries,  start new one
                add_new()

        return <str> fullpath
        return <Path>, <int> tar file path obj, pkg_id
        """
        # pkg_id, filename
        results = self.ctx.db_pkg.select_by_status("OPEN")

        num_open = len(results)

        # open a new one if no pacakges are open
        if num_open == 0:
            tar_path, pkg_id = self.add_new()
        #  should always be only one
        else:
            pkg_id = results[-1]['pkg_id']
            tar_file = f"{results[0]['filepath']}/{results[0]['filename']}"
            tar_path = Path(tar_file)
            if num_open > 1:
                self.log.error(
                    f'WARNING,  more than one OPEN package found, '
                    f'using pkg_id: {pkg_id}, tar_file: {tar_file}'
                )
                self.handle_multiple(pkg_id, results)

        return tar_path, pkg_id

    def add_new(self):
        """
        By adding a record to the database pkg table,  when the next
        observation (koaid) is added,  the new tarfile will be created.

        return <str> fullpath
        return <Path>, <int> tar file path obj, pkg_id
        """

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.ctx.inst}_{self.ctx.lev_str}_{timestamp}.tar.tmp"

        pkg_id = self.ctx.db_pkg.add_new(filename, self.ctx.tar_path)

        # touch the file so it exists
        tar_path = Path(self.ctx.tar_path) / filename

        with tarfile.open(tar_path, "w"):
            pass

        self.log.info(f"New package opened: {filename}")

        return tar_path, pkg_id

    def remove_file(self, tmp_filepath):
        """
        Remove the file from the tar file.

        tmp_filepath <Path> - the Path object of the tar file
        """
        if tmp_filepath.is_file():
            tmp_filepath.unlink(missing_ok=True)

    def included_koaids(self, tar_path):
        """
        Determine the KOAIDS

        return <set> koaids
        """

        koaids = set()

        with tarfile.open(tar_path) as tar:
            for member in tar.getmembers():
                if member.isfile():
                    filename = Path(member.name).name

                    # skip non-fits files
                    if not filename.endswith(".fits"):
                        continue

                    # used for _qramp or other extension fits
                    filename = filename.split("_", 1)[0]

                    # koaid is the first 4 when split by .
                    koaid = ".".join(filename.split(".")[:4])
                    koaids.add(koaid)

        return koaids


    # ----
    # Handle Errors
    # ----
    def handle_missing(self, pkg_id):
        self.log.warning(
            f'pkg_id {pkg_id} does not exist, opening new file and '
            f're-packaging all observations.')

        # reset the observations
        self.ctx.db_obs.reset_by_pkg(pkg_id, 'FILE_MISSING')
        self.log.warning(f'reset pkg_id {pkg_id} missing.')

        # open a new tar
        tar_path, pkg_id = self.add_new()

        return tar_path, pkg_id

    def handle_multiple(self, good_pkg, results):
        for result in results:
            pkg_id = result['pkg_id']
            if pkg_id == good_pkg:
                continue
            self.log.warning(
                f'multiple open files, setting pkg_id {pkg_id} to IGNORE'
                f' and re-packaging observations.'
            )
            self.ctx.db_obs.reset_by_pkg(pkg_id, 'FILE_MULTIPLE')


