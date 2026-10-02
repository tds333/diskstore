"""Tests for the stdlib sqlite3 backend and shared SQL helpers."""

import gc
import os
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
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


def test_quote_literal_rejects_unsupported_type() -> None:
    # Only the SQL types diskstore can bind have a literal form; anything
    # else must fail loudly rather than producing invalid SQL.
    with pytest.raises(TypeError, match="cannot format SQL literal"):
        _sqlite_common.quote_literal(object())


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


def _open_db_fds(dbpath: str) -> int:
    """Number of open file descriptors pointing at *dbpath*."""
    count = 0
    for name in os.listdir(f"/proc/{os.getpid()}/fd"):
        try:
            target = os.readlink(f"/proc/{os.getpid()}/fd/{name}")
        except OSError:
            continue
        if dbpath in target:
            count += 1
    return count


@pytest.mark.skipif(
    not os.path.isdir(f"/proc/{os.getpid()}/fd"),
    reason="requires /proc to count open file descriptors",
)
def test_connection_finalizer_does_not_leak_handles(dbpath) -> None:
    """Garbage-collecting a connection must release its file handle.

    ``connect()`` used to pass a ``factory`` whose ``__del__`` closed the
    connection.  A finalizer runs on the GC thread, and sqlite3 refuses to
    close a connection created by another thread, so ``close()`` raised
    ``ProgrammingError`` and the handle was never released: every
    abandoned per-thread connection leaked a descriptor.  Connections are now
    plain ``sqlite3.Connection``, reclaimed with the object.

    This mirrors DiskStore's per-thread connections, which are dropped
    without an explicit close when a thread ends.
    """
    threads = 20
    baseline = _open_db_fds(dbpath)

    def use(number: int) -> None:
        con = _sqlite_stdlib.connect(dbpath)
        con.execute("CREATE TABLE IF NOT EXISTS t(x)")
        cursor = con.cursor()
        for i in range(5):
            cursor.execute("INSERT INTO t VALUES (?)", (i,))
        # No close(): the thread ends, as with a per-thread DiskStore.

    # Abandoning the connections is the behaviour under test, and sqlite3
    # reports each one as an unclosed database.  That warning is the honest
    # signal for a caller who forgets close(); here it is expected, so it is
    # suppressed to keep the suite output clean.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)

        for number in range(threads):
            thread = threading.Thread(target=use, args=(number,))
            thread.start()
            thread.join()

        gc.collect()
        time.sleep(0.2)

        leaked = _open_db_fds(dbpath) - baseline

    assert leaked < threads // 2


def test_connect_uses_plain_connection(dbpath) -> None:
    """``connect()`` must not install a custom connection subclass.

    The only subclass it ever installed finalised itself in ``__del__``,
    which leaked a descriptor per abandoned thread.  Asserting the plain
    type keeps that from being reintroduced via ``factory=``.
    """
    con = _sqlite_stdlib.connect(dbpath)
    try:
        assert type(con) is sqlite3.Connection
    finally:
        con.close()


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="sqlite3 only rejects a cross-thread close() from 3.11 onwards",
)
def test_finalizer_would_fail_cross_thread(dbpath) -> None:
    """Demonstrates *why* the finalizer is absent, as executable documentation.

    Reproduces the old bug in a throwaway subclass: a connection created on a
    worker thread cannot be closed from the GC thread, so the handle is not
    released.  This test asserts the failure still happens, which is what
    makes the absence of a finalizer in ``connect()`` necessary rather than
    incidental.

    Python 3.10 is excluded: it had no cross-thread check, so ``close()``
    succeeds from the finalizer and there is no bug to demonstrate.  The
    descriptor-counting test above still guards that version.
    """
    failures: list[str] = []

    class Finalizing(sqlite3.Connection):
        def __del__(self) -> None:
            try:
                self.close()
            except Exception as exc:  # recording the failure is the point
                failures.append(f"{type(exc).__name__}: {exc}")

    def use() -> None:
        con = Finalizing(dbpath, isolation_level=None)
        con.execute("CREATE TABLE IF NOT EXISTS t(x)")

    thread = threading.Thread(target=use)
    thread.start()
    thread.join()

    # The connection is abandoned on purpose, so sqlite3's unclosed-database
    # warning is expected here; it is the failure above that is the subject.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)
        gc.collect()
        time.sleep(0.2)

    assert failures, "expected a cross-thread close to fail"
    assert "only be used in that same thread" in failures[0]


def test_busy_error_is_a_driver_error() -> None:
    """BusyError derives from sqlite3.Error, mirroring the apsw shape.

    It is deliberately not an OperationalError subclass: on this backend
    SQLError *is* sqlite3.Error, so BusyError is unavoidably a SQLError here.
    Deriving from OperationalError would add nothing and would imply a
    narrower meaning than apsw's apsw.BusyError, which sits directly under
    apsw.Error.
    """
    assert issubclass(_sqlite_stdlib.BusyError, sqlite3.Error)
    assert not issubclass(_sqlite_stdlib.BusyError, sqlite3.OperationalError)


def test_non_busy_driver_errors_remain_sql_errors() -> None:
    """The classes SQLError exists to catch must not be affected.

    BusyError is a SQLError on this backend by construction, so what matters
    is that real non-busy driver errors keep being SQLErrors.
    """
    assert issubclass(sqlite3.OperationalError, _sqlite_stdlib.SQLError)
    assert issubclass(sqlite3.IntegrityError, _sqlite_stdlib.SQLError)


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


@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="sqlite3 only warns about an unclosed connection from 3.13 onwards",
)
def test_unclosed_connection_warns_like_plain_sqlite3(dbpath) -> None:
    # Abandoning a connection warns, and diskstore's connection behaves the
    # same as plain sqlite3's: both raise ResourceWarning on collection.
    #
    # Python 3.10 to 3.12 are excluded: they did not emit this warning at all,
    # so there is nothing to compare against there.
    #
    # Run in a subprocess on purpose.  The warning is raised from the
    # connection's finalizer, and in-process the object can be kept alive by
    # pytest's rewritten frames or a recorded WarningMessage until
    # interpreter shutdown -- where the warning filters are already torn
    # down, so no in-process filter can suppress it and it is printed
    # after the run.  A subprocess keeps the leak contained.
    script = textwrap.dedent(
        f"""
        import gc, sqlite3, warnings
        con = sqlite3.connect({dbpath!r})
        con.execute("CREATE TABLE IF NOT EXISTS t(x)")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del con
            gc.collect()
        assert any(issubclass(w.category, ResourceWarning) for w in caught)
        print("WARNED")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "WARNED" in result.stdout


def test_closed_connection_does_not_warn(dbpath) -> None:
    # The counterpart: a properly closed connection stays silent, so the
    # warning above is a real signal rather than background noise.  Also run
    # in a subprocess, because an in-process gc.collect() would also collect
    # connections left pending by other tests and attribute their warnings
    # to this test.
    script = textwrap.dedent(
        f"""
        import gc, sqlite3, warnings
        con = sqlite3.connect({dbpath!r})
        con.execute("CREATE TABLE IF NOT EXISTS t(x)")
        con.close()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            gc.collect()
        assert not [w for w in caught if issubclass(w.category, ResourceWarning)]
        print("SILENT")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "SILENT" in result.stdout
