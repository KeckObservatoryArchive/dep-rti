
from unittest.mock import MagicMock, patch

import pytest

from fdt.fdt_xfr_monitor import FdtXfrMonitor


@pytest.fixture
def ctx():

    ctx = MagicMock()

    ctx.cfg = {
        "GENERAL": {
            "max_lock_retries": 3,
            "lock_chk_period": 60,
        },
        "FDT_XFR": {
            "monitor_period": 10,
        },
    }

    return ctx


@pytest.fixture
def monitor(ctx):
    return FdtXfrMonitor(ctx)


@pytest.fixture
def no_sleep():
    with patch("fdt.fdt_xfr_monitor.sleep"):
        yield


def test_init(monitor, ctx):

    assert monitor.ctx == ctx
    assert monitor.stop_requested is False
    assert monitor.max_lock_retries == 3
    assert monitor.lock_chk_period == 60
    assert monitor.monitor_period == 10
    assert monitor.db_pkg == ctx.db_pkg
    assert monitor.xfr == ctx.xfr_fun

def test_stop_handle(monitor, ctx):

    monitor.stop_handle(15, None)

    assert monitor.stop_requested is True

    ctx.log.info.assert_called_once()

def test_run_cleanup(monitor, ctx):

    monitor.stop_requested = True

    monitor.run()

    ctx.lock.release.assert_called_once()
    ctx.proc_conn.close.assert_called_once()
    ctx.lock_conn.close.assert_called_once()

def test_run_lock_failure(monitor, ctx, no_sleep):

    ctx.lock.check.return_value = (False, None)

    monitor.max_lock_retries = 1

    with pytest.raises(SystemExit):

        monitor.run()

def test_run_no_packages(monitor, ctx):

    ctx.lock.check.return_value = (True, 123)

    ctx.db_pkg.ready_to_transfer.return_value = []

    def stop_after_sleep(seconds):
        monitor.stop_requested = True

    with patch(
        "fdt.fdt_xfr_monitor.sleep",
        side_effect=stop_after_sleep
    ):

        monitor.run()


    ctx.xfr_fun.chk_on_startup.assert_called_once()

    ctx.xfr_fun.chk_open_xfr.assert_called()

    ctx.lock.release.assert_called_once()

def test_run_starts_transfer(monitor, ctx):

    ctx.lock.check.return_value = (True, 123)

    pkg = {
        "pkg_id": 10,
        "filename": "test.tar",
        "filepath": "/tmp",
    }

    ctx.db_pkg.ready_to_transfer.return_value = [
        pkg
    ]


    calls = 0

    def stop_after_second_sleep(seconds):
        nonlocal calls

        calls += 1

        if calls >= 1:
            monitor.stop_requested = True


    with patch(
        "fdt.fdt_xfr_monitor.sleep",
        side_effect=stop_after_second_sleep
    ):

        monitor.run()


    ctx.xfr_fun.start_transfer.assert_called_once_with(pkg)


def test_chk_on_startup_called_once(monitor, ctx):

    ctx.lock.check.return_value = (True, 123)

    ctx.db_pkg.ready_to_transfer.return_value = []


    count = 0

    def stop_after_sleep(seconds):
        nonlocal count
        count += 1

        if count == 2:
            monitor.stop_requested = True


    with patch(
        "fdt.fdt_xfr_monitor.sleep",
        side_effect=stop_after_sleep
    ):

        monitor.run()


    ctx.xfr_fun.chk_on_startup.assert_called_once()

def test_run_checks_errors(monitor, ctx):

    ctx.lock.check.return_value = (True, 123)

    ctx.db_pkg.ready_to_transfer.return_value = []


    def stop_after_sleep(seconds):
        monitor.stop_requested = True


    with patch(
        "fdt.fdt_xfr_monitor.sleep",
        side_effect=stop_after_sleep
    ):

        monitor.run()


    ctx.utils.chk_for_errors.assert_called_once_with(
        ctx,
        ctx.db_pkg
    )




