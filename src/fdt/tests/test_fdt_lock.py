from unittest.mock import MagicMock

import pytest

from fdt.fdt_lock import FdtLock


@pytest.fixture
def log():
    return MagicMock()


@pytest.fixture
def conn():
    conn = MagicMock()
    conn.db = MagicMock()
    return conn


@pytest.fixture
def lock(conn, log):
    return FdtLock(
        conn,
        "fdt_test_lock",
        log
    )


# --------------------------------------------------
# acquire()
# --------------------------------------------------

def test_acquire_success(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.side_effect = [
        {"acquired": 1},
        {"id": 123},
        {"owner": 123},
    ]

    result = lock.acquire()

    assert result is True

    conn.ensure_connected.assert_called_once()

    assert cur.execute.call_count == 3

    lock.log.info.assert_called_once()


def test_acquire_failure(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.side_effect = [
        {"acquired": 0},
        {"id": 123},
        {"owner": 456},
    ]

    result = lock.acquire()

    assert result is False


# --------------------------------------------------
# check()
# --------------------------------------------------

def test_check_lock_owned(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.side_effect = [
        {"owner": 123},
        {"id": 123},
    ]

    result = lock.check()

    assert result == (
        True,
        123
    )


def test_check_reacquires_lock(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    # first query: lock owned by another connection
    cur.fetchone.side_effect = [
        {"owner": 999},
        {"id": 123},
    ]

    lock.acquire = MagicMock(return_value=True)

    result = lock.check()

    assert result == (
        True,
        123
    )

    lock.acquire.assert_called_once()


def test_check_failed_reacquire(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.side_effect = [
        {"owner": 999},
        {"id": 123},
    ]

    lock.acquire = MagicMock(return_value=False)

    result = lock.check()

    assert result == (
        False,
        123
    )

    lock.acquire.assert_called_once()


# --------------------------------------------------
# release()
# --------------------------------------------------

def test_release_success(lock, conn):

    cur = MagicMock()

    db = conn.db   # keep reference before release

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.return_value = {
        "released": 1
    }

    lock.release()

    db.close.assert_called_once()

    assert conn.db is None

def test_release_not_held(lock, conn):

    cur = MagicMock()
    db = conn.db

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.return_value = {
        "released": 0
    }

    lock.release()

    lock.log.warning.assert_called_once()

    db.close.assert_called_once()

    assert conn.db is None


def test_release_missing_lock(lock, conn):

    cur = MagicMock()

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.fetchone.return_value = {
        "released": None
    }

    lock.release()

    lock.log.warning.assert_called_once()


def test_release_no_connection(lock, conn):

    conn.db = None

    # should not throw
    lock.release()

    lock.log.assert_not_called()


def test_release_exception(lock, conn):

    cur = MagicMock()
    db = conn.db

    conn.db.cursor.return_value.__enter__.return_value = cur

    cur.execute.side_effect = Exception(
        "database failure"
    )

    lock.release()

    lock.log.exception.assert_called_once()

    db.close.assert_called_once()

    assert conn.db is None
