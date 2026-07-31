
from pathlib import Path
from unittest.mock import MagicMock

from datetime import datetime
import pytest
import tarfile
import yaml

from fdt.fdt_tar_fun import TarFun


@pytest.fixture
def ctx(tmp_path):

    ctx = MagicMock()

    ctx.inst = "SCALES"
    ctx.lev_str = "lev0"

    ctx.tar_path = tmp_path

    ctx.cfg = {
        "SCALES": {
            "max_pkg_size": 100
        },
        "FDT_PKG": {
            "pkg_timeout": 60
        }
    }

    return ctx


@pytest.fixture
def tar_fun(ctx):

    return TarFun(ctx)


def test_init(tar_fun):

    assert tar_fun.retry is False
    assert tar_fun.max_pkg_size == 100

def test_get_file_size(tar_fun, tmp_path):

    file = tmp_path / "test"

    file.write_bytes(b"x" * 1024 * 1024)

    size = tar_fun.get_file_size(2, file)

    assert size == 1.0


def test_add_obs_retries_when_tar_missing(
        tar_fun,
        tmp_path,
        monkeypatch
):

    koaid = "SI.20201027.85635.00"

    fits = tmp_path / f"{koaid}.fits"
    fits.write_text("test")

    tar1 = tmp_path / "first.tar"
    tar2 = tmp_path / "second.tar"

    with tarfile.open(tar1, "w"):
        pass

    with tarfile.open(tar2, "w"):
        pass

    tar_fun.handle_missing = MagicMock(
        return_value=(tar2, 2)
    )

    calls = []

    def fake_get_file_size(pkg_id, tar_path):
        calls.append((pkg_id, tar_path))
        if len(calls) == 1:
            raise FileNotFoundError
        return 12.5

    monkeypatch.setattr(
        tar_fun,
        "get_file_size",
        fake_get_file_size
    )

    tar_fun.ctx.db_pkg.update_size.return_value = 1

    result = tar_fun.add_obs(
        koaid,
        1,
        fits,
        tar1
    )

    assert result == 1

    tar_fun.handle_missing.assert_called_once_with(1)

    assert calls == [
        (1, tar1),
        (2, tar2),
    ]

    tar_fun.ctx.db_pkg.update_size.assert_called_once_with(
        2,
        12.5,
    )


def test_create_cfg(tar_fun, ctx, tmp_path):

    tar_path = tmp_path / "ABC.1.2.3.fits"

    with tarfile.open(tar_path, "w") as tar:

        f = tmp_path / "SI.20251027.85989.00.fits"
        f.write_text("data")

        tar.add(
            f,
            arcname=f.name
        )


    ctx.db_obs.koaids_in_pkg.return_value = [
        {
            "koaid": "SI.20251027.85989.00"
        }
    ]


    cfg_path = tmp_path / "test.cfg"


    tar_fun.create_cfg(
        10,
        cfg_path,
        tar_path
    )


    assert cfg_path.exists()


    data = yaml.safe_load(
        cfg_path.read_text()
    )

    assert data["instrument"] == "SCALES"
    assert data["ingesttype"] == "lev0"
    assert data["filelist"] == [
        "SI.20251027.85989.00"
    ]

def test_add_cfg(tar_fun, ctx, tmp_path):

    tar_path = tmp_path / "test.tar"

    with tarfile.open(tar_path, "w"):
        pass


    ctx.db_obs.koaids_in_pkg.return_value = []


    tar_fun.add_cfg(
        10,
        tar_path
    )


    with tarfile.open(tar_path) as tar:

        assert "test.cfg" in tar.getnames()


    # cfg should be removed after adding
    assert not tar_path.with_suffix(".cfg").exists()

def test_is_valid_true(tar_fun, tmp_path):

    tar_path = tmp_path / "valid.tar"

    with tarfile.open(tar_path, "w"):
        pass


    assert tar_fun.is_valid(tar_path) is True

def test_is_valid_false(tar_fun, tmp_path):

    tar_path = tmp_path / "bad.tar"

    tar_path.write_text(
        "not a tar"
    )


    assert tar_fun.is_valid(tar_path) is False

def test_remove_file(tar_fun, tmp_path):

    file = tmp_path / "test.tar"

    file.touch()

    tar_fun.remove_file(file)

    assert not file.exists()

def test_included_koaids(tar_fun, tmp_path):

    tar_path = tmp_path / "test.tar"

    file = tmp_path / "ABC.1.2.3.fits"

    file.write_text("data")


    with tarfile.open(tar_path, "w") as tar:
        tar.add(
            file,
            arcname=file.name
        )


    result = tar_fun.included_koaids(
        tar_path
    )


    assert result == {
        "ABC.1.2.3"
    }


def test_add_new(tar_fun, tmp_path):

    tar_fun.ctx.inst = "SCALES"
    tar_fun.ctx.lev_str = "lev0"
    tar_fun.ctx.tar_path = tmp_path

    tar_fun.ctx.db_pkg.add_new.return_value = 123

    tar_path, pkg_id = tar_fun.add_new()

    assert pkg_id == 123

    assert tar_path.parent == tmp_path
    assert tar_path.name.startswith("SCALES_lev0_")
    assert tar_path.name.endswith(".tar.tmp")

    assert tar_path.exists()

    # verify it is a valid tar file
    with tarfile.open(tar_path):
        pass

    tar_fun.ctx.db_pkg.add_new.assert_called_once()

    filename = tar_fun.ctx.db_pkg.add_new.call_args.args[0]
    assert filename == tar_path.name

    tar_fun.log.info.assert_called_once_with(
        f"New package opened: {tar_path.name}"
    )


def test_get_current_none_open(tar_fun):

    tar_fun.ctx.db_pkg.select_by_status.return_value = []

    tar_fun.add_new = MagicMock(
        return_value=(Path("/tmp/test.tar.tmp"), 5)
    )

    tar_path, pkg_id = tar_fun.get_current("ABC")

    assert tar_path == Path("/tmp/test.tar.tmp")
    assert pkg_id == 5

    tar_fun.add_new.assert_called_once()


def test_get_current_multiple_open(tar_fun):

    tar_fun.ctx.db_pkg.select_by_status.return_value = [
        {
            "pkg_id": 10,
            "filepath": "/tmp",
            "filename": "first.tar.tmp",
        },
        {
            "pkg_id": 11,
            "filepath": "/tmp",
            "filename": "second.tar.tmp",
        },
    ]

    tar_fun.handle_multiple = MagicMock()

    tar_path, pkg_id = tar_fun.get_current("ABC")

    assert tar_path == Path("/tmp/first.tar.tmp")
    assert pkg_id == 11

    tar_fun.log.error.assert_called_once()

    tar_fun.handle_multiple.assert_called_once_with(
        11,
        tar_fun.ctx.db_pkg.select_by_status.return_value,
    )

def test_get_current_new(tar_fun, ctx):

    ctx.db_pkg.select_by_status.return_value = []

    tar_fun.add_new = MagicMock(
        return_value=(
            "/tmp/new.tar.tmp",
            10
        )
    )


    result = tar_fun.get_current(
        "ABC"
    )


    assert result == (
        "/tmp/new.tar.tmp",
        10
    )

    tar_fun.add_new.assert_called_once()

def test_close_file(tar_fun, ctx, tmp_path):

    old = tmp_path / "test.tar.tmp"
    old.touch()


    result = tar_fun.close_file(
        10,
        old
    )


    assert not old.exists()

    assert (tmp_path / "test.tar").exists()


    ctx.db_pkg.update_filename.assert_called_once()
    ctx.db_obs.update_status_by_pkg.assert_called_once_with(
        10,
        "PACKAGED"
    )

    ctx.db_pkg.closing_time.assert_called_once_with(
        10
    )

def test_need_close_false(tar_fun, ctx, tmp_path):

    file = tmp_path / "test.tar.tmp"
    file.touch()


    ctx.db_pkg.find_filename.return_value = {
        "pkg_id": 1,
        "status": "OPEN",
        "creation_time": datetime.now(),
        "filesize_mb": 100
    }


    assert tar_fun.need_close(file) is True



