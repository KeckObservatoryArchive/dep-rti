
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fdt.fdt_utils import (
    read_config,
    validate_cfg,
    define_data_path,
    define_tar_path,
    chk_for_errors,
)


@pytest.fixture
def valid_cfg():
    return {
        "DATABASE": {
            "host": "localhost",
            "user": "test",
            "pwd": "pwd",
            "db": "fdt",
        },
        "GENERAL": {
            "dev": "false",
            "koa_base_path": "/data/koa",
            "max_errors": 5,
            "max_lock_retries": 3,
            "lock_chk_period": 60,
            "log_dir": "/tmp",
            "admin_email": "some-email.com",
        },
        "FDT_PKG": {
            "pkg_timeout": 30,
            "monitor_period": 10,
        },
        "FDT_XFR": {
            "xfr_timeout": 60,
            "monitor_period": 10,
            "dtn_jar": "/tmp/fdt.jar",
            "dtn_server": "server",
            "dtn_port": "50750",
        },
        "SCALES": {
            "max_pkg_size": 1000,
            "inst_prefixes": ["SCALES"],
        },
    }


@pytest.fixture
def ctx():
    ctx = MagicMock()
    ctx.inst = "SCALES"
    ctx.lev = "lev0"
    ctx.lev_str = "lev0"

    ctx.cfg = {
        "GENERAL": {
            "koa_base_path": "/data/koa"
        }
    }

    return ctx


# --------------------------------------------------
# read_config
# --------------------------------------------------

def test_read_config(tmp_path):

    cfg_file = tmp_path / "test.yaml"

    cfg_file.write_text(
        yaml.safe_dump({
            "TEST": "value"
        })
    )

    result = read_config(cfg_file)

    assert result == {
        "TEST": "value"
    }


# --------------------------------------------------
# define_data_path
# --------------------------------------------------

def test_define_data_path_override(ctx):

    result = define_data_path(
        ctx,
        "/override/path"
    )

    assert result == Path("/override/path")


def test_define_data_path_default(ctx):

    result = define_data_path(
        ctx,
        None
    )

    assert result == Path(
        "/data/koa/SCALES/lev0/"
    )


# --------------------------------------------------
# define_tar_path
# --------------------------------------------------

def test_define_tar_path_override(ctx):

    result = define_tar_path(
        ctx,
        "/tmp/tars"
    )

    assert result == Path("/tmp/tars")


def test_define_tar_path_default(ctx):

    result = define_tar_path(
        ctx,
        None
    )

    assert result == Path(
        "/data/koa/SCALES/tarfiles/lev0/"
    )


# --------------------------------------------------
# chk_for_errors
# --------------------------------------------------

def test_chk_for_errors_no_errors():

    ctx = MagicMock()
    ctx.dev = False

    db = MagicMock()
    db.chk_for_errors.return_value = []

    chk_for_errors(ctx, db)

    ctx.log.error.assert_not_called()


def test_chk_for_errors_dev_mode():

    ctx = MagicMock()
    ctx.dev = True
    ctx.admin_email = "admin@test.com"

    db = MagicMock()
    db.chk_for_errors.return_value = [
        {"pkg_id": 1}
    ]

    with patch(
        "fdt.fdt_utils.check_dep_status_errors.main"
    ) as mock_error:

        chk_for_errors(ctx, db)

    mock_error.assert_called_once_with(
        admin_email="admin@test.com",
        slack=False,
        dev=True
    )


def test_chk_for_errors_production_mode():

    ctx = MagicMock()
    ctx.dev = False
    ctx.admin_email = "admin@test.com"

    db = MagicMock()
    db.chk_for_errors.return_value = [
        {"pkg_id": 1}
    ]

    with patch(
        "fdt.fdt_utils.check_dep_status_errors.main"
    ) as mock_error:

        chk_for_errors(ctx, db)

    mock_error.assert_called_once_with(
        admin_email="admin@test.com",
        slack=True,
        dev=False
    )

