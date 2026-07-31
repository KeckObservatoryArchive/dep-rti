from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import signal

import psutil

from fdt.fdt_xfr_fun import FdtXfrFun, XfrProcess


@pytest.fixture
def ctx():

    ctx = MagicMock()

    ctx.cfg = {
        "FDT_XFR": {
            "dtn_jar": "/koa/fdt.jar",
            "dtn_server": "server",
            "dtn_port": "88888",
            "xfr_timeout": 120,
        },
        "GENERAL": {
            "admin_email": "test@test.com",
        },
    }

    return ctx


@pytest.fixture
def xfr(ctx):
    return FdtXfrFun(ctx)

def test_init(xfr, ctx):

    assert xfr.ctx == ctx
    assert xfr.stop_requested is False
    assert xfr.active_xfr == set()
    assert xfr.pid_finished == set()
    assert xfr.pid_error == set()

def test_stop_handle(xfr, ctx):

    xfr.stop_handle(signal.SIGTERM, None)

    assert xfr.stop_requested is True
    ctx.log.info.assert_called_once()

@patch("fdt.fdt_xfr_fun.subprocess.Popen")
def test_transfer(mock_popen, xfr):

    proc = MagicMock()
    proc.pid = 123
    mock_popen.return_value = proc

    result = xfr.transfer(
        [
            "/tmp/test.tar",
            "/tmp/test.complete"
        ]
    )

    assert result == proc

    cmd = mock_popen.call_args.args[0]

    assert "/usr/bin/java -jar /koa/fdt.jar" in cmd
    assert "-c server" in cmd
    assert "-p 88888" in cmd
    assert "/tmp/test.tar" in cmd
    assert "/tmp/test.complete" in cmd
    assert "&&" in cmd

    mock_popen.assert_called_once_with(
        cmd,
        shell=True,
        start_new_session=True
    )

@patch.object(FdtXfrFun, "transfer")
def test_transfer_pkg(mock_transfer, xfr, ctx, tmp_path):

    tar = tmp_path / "test.tar"
    tar.touch()

    mock_transfer.return_value = "PROC"

    result = xfr.transfer_pkg(
        10,
        tar
    )

    assert result == "PROC"

    ctx.tar_fun.add_cfg.assert_called_once_with(
        10,
        tar
    )

    assert tar.with_suffix(".complete").exists()

    mock_transfer.assert_called_once()

@patch("fdt.fdt_xfr_fun.psutil.Process")
@patch.object(FdtXfrFun, "transfer_pkg")
def test_start_transfer(
        mock_transfer,
        mock_process,
        xfr,
        ctx):

    proc = MagicMock()
    proc.pid = 123

    mock_transfer.return_value = proc

    ps_proc = MagicMock()
    ps_proc.create_time.return_value = datetime.now().timestamp()

    mock_process.return_value = ps_proc


    pkg = {
        "pkg_id": 5,
        "filepath": "/tmp",
        "filename": "test.tar",
    }

    xfr.start_transfer(pkg)


    assert len(xfr.active_xfr) == 1

    ctx.db_pkg.update_pid.assert_called_once()

    ctx.db_obs.update_status_by_pkg.assert_called_once_with(
        5,
        "TRANSFERRING"
    )


@patch("fdt.fdt_xfr_fun.psutil.Process")
def test_is_running_true(mock_process, xfr):

    start = datetime.now()

    proc = MagicMock()
    proc.is_running.return_value = True
    proc.create_time.return_value = start.timestamp()

    mock_process.return_value = proc


    assert xfr.is_running(
        123,
        start
    ) is True

@patch("fdt.fdt_xfr_fun.psutil.Process")
def test_is_running_false_missing(mock_process, xfr):

    mock_process.side_effect = psutil.NoSuchProcess(123)

    assert xfr.is_running(
        123,
        datetime.now()
    ) is False

def test_handle_complete_transfer(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now()
    )

    xfr.handle_complete_transfer(obj)


    ctx.db_pkg.update_transferred.assert_called_once()

    ctx.db_obs.update_status_by_pkg.assert_called_once_with(
        10,
        "TRANSFERRED"
    )

def test_handle_failed_transfer(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now()
    )

    xfr.handle_failed_transfer(obj)

    ctx.db_pkg.update_error.assert_called_once_with(
        10,
        "ERROR",
        "TRANSFER_FAILED"
    )

def test_handle_timeout(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now()
    )


    xfr.handle_timeout(obj)


    ctx.db_pkg.update_error.assert_called_once_with(
        10,
        "ERROR",
        "TRANSFER_TIMEOUT"
    )

def test_chk_open_xfr_running(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now(),
        proc=MagicMock()
    )

    obj.proc.poll.return_value = None

    xfr.active_xfr.add(obj)

    ctx.db_pkg.expired_transfers.return_value = []


    xfr.chk_open_xfr()


    assert obj in xfr.active_xfr

def test_chk_open_xfr_complete(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now(),
        proc=MagicMock()
    )

    obj.proc.poll.return_value = 0

    xfr.active_xfr.add(obj)

    ctx.db_pkg.expired_transfers.return_value = []


    xfr.chk_open_xfr()


    ctx.db_pkg.update_transferred.assert_called_once()

def test_chk_open_xfr_failed(xfr, ctx):

    obj = XfrProcess(
        pid=123,
        pkg_id=10,
        start_time=datetime.now(),
        proc=MagicMock()
    )

    obj.proc.poll.return_value = 1

    xfr.active_xfr.add(obj)

    ctx.db_pkg.expired_transfers.return_value = []


    xfr.chk_open_xfr()


    ctx.db_pkg.update_error.assert_called_once()

def test_chk_on_startup_missing_process(xfr, ctx):

    ctx.db_pkg.select_by_status.return_value = [
        {
            "pkg_id": 10,
            "filename": "test.tar",
            "filepath": "/tmp",
            "xfr_pid": 123,
            "xfr_start_time": datetime.now()
        }
    ]

    with patch.object(
        xfr,
        "is_running",
        return_value=False
    ):
        xfr.chk_on_startup()

    ctx.db_obs.set_unknown.assert_called_once_with(
        10,
        "TRANSFERRING"
    )


