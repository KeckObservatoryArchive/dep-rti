from unittest.mock import MagicMock, patch

import pymysql

from fdt.fdt_database import DatabaseConnect


def test_init():
    cfg = {
        "user": "testuser",
        "pwd": "password",
        "db": "testdb",
    }

    db = DatabaseConnect(cfg)

    assert db.db_cfg == cfg
    assert db.db is None


@patch("fdt.fdt_database.pymysql.connect")
def test_connect(mock_connect):
    cfg = {
        "user": "testuser",
        "pwd": "password",
        "db": "testdb",
    }

    mock_db = MagicMock()
    mock_connect.return_value = mock_db

    db = DatabaseConnect(cfg)

    db.connect()

    assert db.db == mock_db

    mock_connect.assert_called_once_with(
        user="testuser",
        password="password",
        database="testdb",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        client_flag=pymysql.constants.CLIENT.FOUND_ROWS,
    )


@patch("fdt.fdt_database.pymysql.connect")
def test_ensure_connected_when_none(mock_connect):
    cfg = {
        "user": "user",
        "pwd": "pwd",
        "db": "db",
    }

    mock_db = MagicMock()
    mock_connect.return_value = mock_db

    db = DatabaseConnect(cfg)

    db.ensure_connected()

    mock_connect.assert_called_once()
    assert db.db == mock_db


def test_ensure_connected_existing_connection():
    cfg = {
        "user": "user",
        "pwd": "pwd",
        "db": "db",
    }

    mock_db = MagicMock()

    db = DatabaseConnect(cfg)
    db.db = mock_db

    db.ensure_connected()

    mock_db.ping.assert_called_once_with(reconnect=True)


def test_close_connection():
    cfg = {
        "user": "user",
        "pwd": "pwd",
        "db": "db",
    }

    mock_db = MagicMock()

    db = DatabaseConnect(cfg)
    db.db = mock_db

    db.close()

    mock_db.close.assert_called_once()
    assert db.db is None


def test_close_when_not_connected():
    cfg = {
        "user": "user",
        "pwd": "pwd",
        "db": "db",
    }

    db = DatabaseConnect(cfg)

    # should not raise
    db.close()

    assert db.db is None


def test_close_sets_db_none_even_if_close_fails():
    cfg = {
        "user": "user",
        "pwd": "pwd",
        "db": "db",
    }

    mock_db = MagicMock()
    mock_db.close.side_effect = Exception("close failed")

    db = DatabaseConnect(cfg)
    db.db = mock_db

    try:
        db.close()
    except Exception:
        pass

    assert db.db is None