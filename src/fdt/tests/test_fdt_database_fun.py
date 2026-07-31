import logging
from unittest.mock import MagicMock

import pytest

from fdt.fdt_database_fun import FdtDatabaseFun, PkgTable, ObsTable


@pytest.fixture
def conn():
    conn = MagicMock()
    conn.ensure_connected = MagicMock()

    cursor = MagicMock()
    conn.db.cursor.return_value.__enter__.return_value = cursor

    return conn


@pytest.fixture
def cursor(conn):
    return conn.db.cursor.return_value.__enter__.return_value


@pytest.fixture
def log():
    return logging.getLogger("test")


def test_exec_fetch_all(conn, cursor, log):
    cursor.fetchall.return_value = [{"id": 1}]

    db = FdtDatabaseFun(conn, log)

    result = db._exec_q(
        "SELECT * FROM x",
        qtype=db.fetch_all,
    )

    assert result == [{"id": 1}]
    cursor.execute.assert_called_once_with("SELECT * FROM x", ())


def test_exec_fetch_one(conn, cursor, log):
    cursor.fetchone.return_value = {"id": 5}

    db = FdtDatabaseFun(conn, log)

    result = db._exec_q(
        "SELECT",
        qtype=db.fetch_one,
    )

    assert result["id"] == 5

def test_exec_last_rowid(conn, cursor, log):
    cursor.lastrowid = 99

    db = FdtDatabaseFun(conn, log)

    result = db._exec_q(
        "INSERT",
        qtype=db.last_row_id,
    )

    assert result == 99


def test_exec_rowcount(conn, cursor, log):
    cursor.rowcount = 3

    db = FdtDatabaseFun(conn, log)

    assert db._exec_q("UPDATE") == 3

@pytest.fixture
def pkg(conn, log):
    return PkgTable("SCALES", "lev0", conn, log)


def test_select_by_status(pkg, cursor):

    cursor.fetchall.return_value = [{"pkg_id": 1}]

    result = pkg.select_by_status("OPEN")

    assert result == [{"pkg_id": 1}]

    sql, params = cursor.execute.call_args.args

    assert "WHERE STATUS=%s" in sql
    assert params == ("OPEN", "SCALES", "lev0")


def test_update_status(pkg, cursor):

    cursor.rowcount = 1

    pkg.update_status(10, "CLOSED")

    sql, params = cursor.execute.call_args.args

    assert "UPDATE fdt_packages" in sql
    assert params == ("CLOSED", 10)

def test_add_new_insert(pkg, cursor):

    cursor.fetchone.return_value = None
    cursor.lastrowid = 17

    pkg_id = pkg.add_new("a.tar", "/tmp")

    assert pkg_id == 17

    assert cursor.execute.call_count == 2

def test_add_new_reuse(pkg, cursor):

    cursor.fetchone.return_value = {"pkg_id": 8}

    pkg_id = pkg.add_new("a.tar", "/tmp")

    assert pkg_id == 8

    assert cursor.execute.call_count == 2

def test_update_size(pkg, cursor):

    cursor.rowcount = 1

    pkg.update_size(5, 123)

    _, params = cursor.execute.call_args.args

    assert params == (123, 5)

# OBSTABLE

@pytest.fixture
def obs(conn, log):
    return ObsTable("SCALES", "lev0", conn, log)

def test_filepath(obs, cursor):

    cursor.fetchone.return_value = {
        "filepath": "/data/a.fits",
        "filepath_replacement": None,
    }

    assert obs.filepath_by_koaid("KOA") == "/data/a.fits"

def test_filepath_replacement(obs, cursor):

    cursor.fetchone.return_value = {
        "filepath": "/data/a",
        "filepath_replacement": "/scratch/a",
    }

    assert obs.filepath_by_koaid("KOA") == "/scratch/a"

def test_search_koaids(obs, cursor):

    cursor.fetchall.return_value = []

    obs.search_koaids(["A", "B", "C"])

    sql, params = cursor.execute.call_args.args

    assert "IN (%s, %s, %s)" in sql
    assert params == ["A", "B", "C"]


def test_insert_obs(obs, cursor):

    cursor.lastrowid = 55

    obsid = obs.insert_obs(
        "KOA1",
        "/tmp/file",
        "PENDING",
    )

    assert obsid == 55






