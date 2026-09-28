"""Tests for the stdlib sqlite3 backend and shared SQL helpers."""

import gc
import sqlite3
import tempfile
import warnings

import pytest

from diskstore import _sqlite_common, _sqlite_stdlib


@pytest.fixture
def dbpath():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as handle:
        return handle.name


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (True, "1"),
        (False, "0"),
        (5, "5"),
        (-3, "-3"),
        (1.5, "1.5"),
        (b"\x00\xff", "X'00ff'"),
        ("O'Brien", "'O''Brien'"),
    ],
)
def test_quote_literal_values(value, expected) -> None:
    assert _sqlite_common.quote_literal(value) == expected


def test_format_sql_value_alias() -> None:
    assert _sqlite_common.format_sql_value is _sqlite_common.quote_literal


def test_set_pragma_rejects_invalid_identifier(dbpath) -> None:
    con = _sqlite_stdlib.connect(dbpath)
    try:
        with pytest.raises(ValueError, match="invalid pragma name"):
            _sqlite_stdlib.set_pragma(con, "cache_size; DROP TABLE t", 1)
        with pytest.raises(ValueError, match="invalid pragma name"):
            _sqlite_stdlib.get_pragma(con, "cache_size; DROP TABLE t")
    finally:
        con.close()


def test_set_get_pragma_roundtrip(dbpath) -> None:
    con = _sqlite_stdlib.connect(dbpath)
    try:
        _sqlite_stdlib.set_pragma(con, "cache_size", -4096)
        assert _sqlite_stdlib.get_pragma(con, "cache_size") == -4096
    finally:
        con.close()


def test_connect_is_autocommit(dbpath) -> None:
    con = _sqlite_stdlib.connect(dbpath)
    try:
        assert con.isolation_level is None
        con.execute("CREATE TABLE t(x)")
    finally:
        con.close()
    other = _sqlite_stdlib.connect(dbpath)
    try:
        names = {row[0] for row in other.execute("SELECT name FROM sqlite_master")}
        assert "t" in names
    finally:
        other.close()


def test_connect_timeout_sets_busy_timeout(dbpath) -> None:
    con = _sqlite_stdlib.connect(dbpath, timeout=0.25)
    try:
        assert _sqlite_stdlib.get_pragma(con, "busy_timeout") == 250
    finally:
        con.close()


def test_connect_readonly_rejects_writes(dbpath) -> None:
    writer = _sqlite_stdlib.connect(dbpath)
    writer.execute("CREATE TABLE t(x)")
    writer.close()

    con = _sqlite_stdlib.connect(dbpath, readonly=True)
    try:
        with pytest.raises(sqlite3.OperationalError):
            con.execute("INSERT INTO t VALUES (1)")
    finally:
        con.close()


def test_table_columns(dbpath) -> None:
    con = _sqlite_stdlib.connect(dbpath)
    try:
        con.execute("CREATE TABLE t(a INTEGER NOT NULL, b TEXT)")
        names = [row[1] for row in _sqlite_stdlib.table_columns(con, "t")]
        assert names == ["a", "b"]
        notnull = {row[1]: row[3] for row in _sqlite_stdlib.table_columns(con, "t")}
        assert notnull == {"a": 1, "b": 0}
    finally:
        con.close()


def test_busy_error_is_operational_error() -> None:
    assert issubclass(_sqlite_stdlib.BusyError, sqlite3.OperationalError)


def test_check_version_rejects_old_sqlite(monkeypatch) -> None:
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 31, 0))
    with pytest.raises(RuntimeError, match=r"3\.35"):
        _sqlite_stdlib.check_version()


def test_check_version_accepts_supported(monkeypatch) -> None:
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 35, 0))
    _sqlite_stdlib.check_version()


def test_is_busy_from_message_without_errorcode() -> None:
    # Python 3.10's sqlite3 exposes no sqlite_errorname on raised errors,
    # so busy detection falls back to the message.
    assert _sqlite_stdlib.is_busy(sqlite3.OperationalError("database is locked"))


def test_is_busy_false_for_other_operational_error() -> None:
    assert not _sqlite_stdlib.is_busy(sqlite3.OperationalError("no such table: t"))


def test_connection_is_closed_on_gc(dbpath) -> None:
    # Worker-thread connections (and doc examples) may be released without an
    # explicit close; the connection must self-close to avoid leaking the
    # file handle and emitting ResourceWarning.
    con = _sqlite_stdlib.connect(dbpath)
    con.execute("CREATE TABLE t(x)")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del con
        gc.collect()
    resource_warnings = [w for w in caught if issubclass(w.category, ResourceWarning)]
    assert not resource_warnings
