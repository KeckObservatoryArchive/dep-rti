from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fdt.fdt_pkg_monitor import FdtPkgMonitor


@pytest.fixture
def ctx():
    ctx = MagicMock()

    ctx.cfg = {
        "GENERAL": {
            "max_errors": 5,
            "max_lock_retries": 3,
            "lock_chk_period": 60,
        },
        "FDT_PKG": {
            "monitor_period": 10,
        },
    }

    return ctx


@pytest.fixture
def monitor(ctx):
    return FdtPkgMonitor(ctx)

@pytest.fixture
def no_sleep():
    with patch("fdt.fdt_pkg_monitor.sleep"):
        yield


def test_init(monitor, ctx):

    assert monitor.ctx == ctx
    assert monitor.stop_requested is False
    assert monitor.max_errors == 5
    assert monitor.max_lock_retries == 3
    assert monitor.lock_chk_period == 60
    assert monitor.monitor_period == 10

def test_stop_handle(monitor, ctx):

    monitor.stop_handle(15, None)

    assert monitor.stop_requested is True
    ctx.log.info.assert_called_once()

def test_process_observations_no_files(monitor, ctx):

    ctx.pkg_fun.chk_for_new_files.return_value = []

    result = monitor.process_observations(set())

    assert result == set()

    ctx.pkg_fun.proc_obs.assert_not_called()

def test_process_observations_one_file(monitor, ctx):

    ctx.pkg_fun.chk_for_new_files.return_value = [
        {"koaid": "ABC.123"}
    ]

    ctx.pkg_fun.proc_obs.return_value = Path("/tmp/test.tar")

    result = monitor.process_observations(set())

    assert result == {Path("/tmp/test.tar")}

    ctx.db_obs.update_start_time.assert_called_once_with(
        "ABC.123"
    )

    ctx.db_obs.update_end_time.assert_called_once_with(
        "ABC.123"
    )

    ctx.pkg_fun.proc_obs.assert_called_once_with(
        "ABC.123"
    )

def test_process_observations_no_tar(monitor, ctx):

    ctx.pkg_fun.chk_for_new_files.return_value = [
        {"koaid": "ABC"}
    ]

    ctx.pkg_fun.proc_obs.return_value = None

    result = monitor.process_observations(set())

    assert result == set()

def test_process_observations_stop_requested(monitor, ctx):

    monitor.stop_requested = True

    ctx.pkg_fun.chk_for_new_files.return_value = [
        {"koaid": "ABC"}
    ]

    result = monitor.process_observations(set())

    assert result == set()

    ctx.pkg_fun.proc_obs.assert_not_called()

def test_chk_finalize_tarfiles_removes_closed(monitor, ctx):

    tar1 = Path("/tmp/a.tar")
    tar2 = Path("/tmp/b.tar")

    ctx.tar_fun.need_close.side_effect = [
        True,
        False
    ]

    result = monitor.chk_finalize_tarfiles(
        {tar1, tar2}
    )

    assert result == {tar2}

def test_chk_finalize_tarfiles_keeps_open(monitor, ctx):

    tar = Path("/tmp/a.tar")

    ctx.tar_fun.need_close.return_value = False

    result = monitor.chk_finalize_tarfiles({tar})

    assert result == {tar}

def test_run_releases_resources(monitor, ctx):

    ctx.lock.check.return_value = (True, 123)

    ctx.pkg_fun.startup_clean.return_value = None
    ctx.pkg_fun.chk_for_new_files.return_value = []

    monitor.stop_requested = True

    monitor.run()

    ctx.lock.release.assert_called_once()
    ctx.proc_conn.close.assert_called_once()
    ctx.lock_conn.close.assert_called_once()

def test_run_exits_if_lock_unavailable(monitor, ctx, no_sleep):

    ctx.lock.check.return_value = (False, None)

    monitor.max_lock_retries = 1

    with pytest.raises(SystemExit):
        monitor.run()




