"""
The FDT Processing TAR file functions.
"""
import os
import glob
import tarfile
from pathlib import Path
from datetime import datetime, timedelta

class TarFun:

    def __init__(self, ctx):
        self.ctx = ctx
        self.log = ctx.log

        self.retry = False
        self.max_pkg_size = ctx.cfg[ctx.inst]['max_pkg_size']
        self.time_limit = timedelta(minutes=ctx.cfg['FDT_PROCESS']['pkg_timeout'])


    def add_obs(self, koaid, pkg_id, obs_path, tar_full_path):
        """
        Add all the files with a KOAID to a tar file

        tar_full_path <str>: full path of the tar file including file name
        """
        # TODO maybe better to have a data products list for each
        #  instrument (otherwise might get back up files,  etc)

        try:
            obs_dir = Path(obs_path).parent
            with tarfile.open(tar_full_path, "a") as tar:
                for filename in glob.glob(f"{obs_dir}/*{koaid}*"):
                    self.log.debug(f'Adding: {filename} to {tar_full_path}')
                    tar.add(filename)
        except Exception as err:
            self.log.error(f"Failed to add {koaid} to {obs_path}: {err}")
            return None

        tar_mb = self.get_file_size(tar_full_path)

        self.log.debug(f"File size, {tar_full_path}: {tar_mb} MB")

        # this updates the file size and the KOAID obs_count by 1
        num_updated = self.ctx.db_pkg.update_size(pkg_id, tar_mb, update_cnt=1)

        return num_updated


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
        except Exception:
            self.log.exception(f"Could not remove {filename}")
            raise

        # update the package status to IGNORE
        num = self.ctx.db_pkg.update_status(pkg_id, 'IGNORE')

        return


    def chk_finalize(self, filepath_obj):
        """
        Check if the tar needs to be closed.  If it meets the full criteria,
        finalize it by (1) updating the DB status (2)

        filepath_obj <Path> - the Path object of the tar file
        pkg_id <int> - the pkg_id in the database

        """

        # fetch one
        result = self.ctx.db_pkg.find_open_filename(filepath_obj.name)

        if not result:
            self.log.error(f"Could not find {filepath_obj.name}")
            return False

        pkg_id = result['pkg_id']
        create_time = result['creation_time']
        size_mb = result['filesize_mb']

        # TODO confirm the database size is correct
        tar_mb = self.get_file_size(filepath_obj)

        if tar_mb > size_mb:
            self.ctx.db_pkg.update_size(tar_mb)
            size_mb = tar_mb

        now = datetime.now()

        # close_requested used by CLI
        # check size limit (TODO could minus tar packaging,  but seems minimal)
        # check temporal limit (TODO could include the 5s check period)
        rules = [
            result['status'] == 'CLOSE_REQUESTED',
            size_mb >= self.max_pkg_size,
            now - create_time >= self.time_limit
        ]

        self.log.debug(f"rule1: {result['status'] == 'CLOSE_REQUESTED'}")
        self.log.debug(
            f"rule2: {size_mb >= self.max_pkg_size}, {size_mb}, "
            f"{self.max_pkg_size}"
        )
        self.log.debug(f"rule3: {now - create_time >= self.time_limit}")

        if not any(rules):
            return False

        self.close_file(pkg_id, filepath_obj)

        return True


    def close_file(self, pkg_id, tar_filepath):
        """
        Rename the file from .tar.tmp to .tar

        Update the name in the database.

        tar_filepath <Path> - the Path object of the tar file
        """
        # move .tar.tmp -> .tar
        new_filepath = tar_filepath.with_suffix("")
        tar_filepath.replace(new_filepath)

        # update the database
        new_filename = tar_filepath.with_suffix("").name
        num = self.ctx.db_pkg.update_filename(pkg_id, new_filename)

        # mark the other pkgs as deleted
        self.ctx.db_pkg.mark_prev_deleted(pkg_id)

        # set the status of the pkg observations to PACKAGED
        num = self.ctx.db_obs.update_status_by_pkg(pkg_id, "PACKAGED")

        # update the close time for metrics
        self.ctx.db_pkg.closing_time(pkg_id)

        return num


    def get_current(self, koaid):

        """
        select pkg_id, filename from fdt_packages where status = OPEN

        if no currently packaging tar entries,  start new one
                add_new()

        return <str> fullpath
        return <int> pkg_id
        """
        tar_file = None
        pkg_id = None

        # pkg_id, filename
        results = self.ctx.db_pkg.select_by_status("OPEN")

        num_open = len(results)

        # open a new one if no pacakges are open
        if num_open == 0:
            tar_file, pkg_id = self.add_new(koaid)
        #  should always be only one
        elif num_open == 1:
            pkg_id = results[0]['pkg_id']
            tar_file = f"{results[0]['filepath']}/{results[0]['filename']}"
        else:
            # TODO email warning / handle
            self.log.error('WARNING,  more than one OPEN package found')

        return tar_file, pkg_id


    def add_new(self, koaid):
        """
        By adding a record to the database pkg table,  when the next
        observation (koaid) is added,  the new tarfile will be created.

        return <str> fullpath
        return <int> pkg_id
        """

        filename = f"{koaid}_{self.ctx.lev_str}.tar.tmp"

        pkg_id = self.ctx.db_pkg.add_new(filename, self.ctx.tar_path)

        return f"{self.ctx.tar_path}/{filename}", pkg_id

