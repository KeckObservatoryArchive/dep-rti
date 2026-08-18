

import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fdt.fdt_pkg_fun import FdtPkgFun


@pytest.fixture
def ctx(tmp_path):
    ctx = MagicMock()

    ctx.log = logging.getLogger("test")
    ctx.cfg = {"GENERAL": {"admin_email": "lfuhrman@keck.hawaii.edu"}}

    ctx.db_obs = MagicMock()
    ctx.db_pkg = MagicMock()
    ctx.tar_fun = MagicMock()

    ctx.inst = "SCALES"
    ctx.lev_str = "lev0"
    ctx.tar_path = tmp_path

    return ctx


@pytest.fixture
def pkg(ctx):
    return FdtPkgFun(ctx)


def test_proc_obs_success(pkg, ctx):
    ctx.db_obs.filepath_by_koaid.return_value = "/data/file.fits"
    ctx.tar_fun.get_current.return_value = ("/tmp/a.tar.tmp", 17)
    ctx.tar_fun.add_obs.return_value = 1

    obs_name = "SI.20251027.85635.00"
    pkg.proc_obs(obs_name)

    ctx.db_obs.update_status_by_koaid.assert_called_once_with(
        obs_name, "PACKAGING"
    )

    ctx.db_obs.update_pkg_id.assert_called_once_with(17, obs_name)
    ctx.db_obs.set_pkgd.assert_called_once_with(obs_name)


def test_proc_obs_add_obs_failed(pkg, ctx):
    ctx.db_obs.filepath_by_koaid.return_value = "/tmp/file"
    ctx.tar_fun.get_current.return_value = ("x.tar", 9)
    ctx.tar_fun.add_obs.return_value = 0

    assert pkg.proc_obs("KOA") is None

    ctx.db_obs.update_pkg_id.assert_not_called()
    ctx.db_obs.set_pkgd.assert_not_called()

def test_chk_for_new_files(pkg, ctx):
    ctx.db_obs.select_by_status.return_value = ["a", "b"]

    assert pkg.chk_for_new_files() == ["a", "b"]

    ctx.db_obs.select_by_status.assert_called_once_with("PENDING")



def test_get_tmp_tarfiles(pkg, ctx):
    good = ctx.tar_path / "SCALES_test.lev0.tar.tmp"
    bad = ctx.tar_path / "abc.txt"

    print(ctx.tar_path)

    good.touch()
    bad.touch()

    files = pkg.get_tmp_tarfiles()

    assert files == [good]

def test_chk_open_pkgs_valid(pkg, ctx, tmp_path):
    tar = tmp_path / "abc.tar"
    tar.touch()

    ctx.db_pkg.select_by_status.side_effect = [
        [
            {
                "pkg_id": 4,
                "filename": tar.name,
                "filepath": str(tmp_path),
            }
        ],
        [],
    ]

    result = pkg.chk_open_pkgs()

    assert result == set()

    ctx.db_pkg.update_status.assert_called_with(4, "CLOSED")
    ctx.db_obs.update_status_by_pkg.assert_called_with(
        4,
        "PACKAGED",
    )

def test_chk_tmp_tarfiles_close(pkg, ctx, tmp_path):
    tar = tmp_path / "a.tar.tmp"
    tar.touch()

    ctx.db_pkg.select_by_status.return_value = [
        {
            "pkg_id": 1,
            "filename": tar.name,
            "filepath": str(tmp_path),
        }
    ]

    pkg.chk_tmp_tarfiles()

    ctx.tar_fun.close_file.assert_called_once_with(1, tar)

def test_chk_tmp_tarfiles_missing(pkg, ctx, tmp_path):
    ctx.db_pkg.select_by_status.return_value = [
        {
            "pkg_id": 1,
            "filename": "missing.tar.tmp",
            "filepath": str(tmp_path),
        }
    ]

    pkg.chk_tmp_tarfiles()

    ctx.db_pkg.update_status.assert_called_once_with(
        1,
        "IGNORE",
    )

def test_startup_clean(pkg):
    pkg.get_tmp_tarfiles = MagicMock(side_effect=[[], []])
    pkg.chk_open_pkgs = MagicMock(return_value=[])
    pkg.chk_tmp_tarfiles = MagicMock()

    pkg.startup_clean()

    pkg.chk_open_pkgs.assert_called_once()
    pkg.chk_tmp_tarfiles.assert_called_once()
    pkg.get_tmp_tarfiles.assert_called()
    pkg.ctx.db_obs.change_status.assert_called_once_with(
        "PACKAGING",
        "PENDING",
    )

