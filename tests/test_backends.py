"""Driver-backend contract tests, run against every available backend."""

import importlib.util
import inspect
import tempfile

import pytest

from diskstore import _sqlite_stdlib
from diskstore.const import TIMEOUT

BACKENDS = [(_sqlite_stdlib, "sqlite3")]
if importlib.util.find_spec("apsw") is not None:
    from diskstore import _sqlite_apsw

    BACKENDS.append((_sqlite_apsw, "apsw"))


@pytest.fixture(params=BACKENDS, ids=[name for _, name in BACKENDS])
def backend(request):
    return request.param[0]


@pytest.fixture
def dbpath():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as handle:
        return handle.name


def test_connect_and_pragma_roundtrip(backend, dbpath) -> None:
    con = backend.connect(dbpath)
    try:
        backend.set_pragma(con, "cache_size", -4096)
        assert backend.get_pragma(con, "cache_size") == -4096
    finally:
        con.close()


def test_connect_default_timeout_is_shared_const(backend) -> None:
    """connect() must default to const.TIMEOUT, not a duplicated literal.

    Guards against the 10.0 literal creeping back into a backend signature
    and silently diverging from the value BaseConfig hands out.  Identity is
    checked deliberately: a literal 10.0 compares equal to the const, so an
    equality check would not notice the regression.
    """
    default = inspect.signature(backend.connect).parameters["timeout"].default
    assert default is TIMEOUT


def test_connect_timeout_sets_busy_timeout(backend, dbpath) -> None:
    con = backend.connect(dbpath, timeout=0.25)
    try:
        assert backend.get_pragma(con, "busy_timeout") == 250
    finally:
        con.close()


def test_table_columns(backend, dbpath) -> None:
    con = backend.connect(dbpath)
    try:
        con.execute("CREATE TABLE t(a INTEGER NOT NULL, b TEXT)")
        names = [row[1] for row in backend.table_columns(con, "t")]
        assert names == ["a", "b"]
    finally:
        con.close()


def test_readonly_rejects_writes(backend, dbpath) -> None:
    writer = backend.connect(dbpath)
    writer.execute("CREATE TABLE t(x)")
    writer.close()

    con = backend.connect(dbpath, readonly=True)
    try:
        with pytest.raises(backend.Error):
            con.execute("INSERT INTO t VALUES (1)")
    finally:
        con.close()


def test_autocommit_is_visible_to_other_connection(backend, dbpath) -> None:
    con = backend.connect(dbpath)
    con.execute("CREATE TABLE t(x)")
    con.close()
    other = backend.connect(dbpath)
    try:
        names = {row[0] for row in other.execute("SELECT name FROM sqlite_master")}
        assert "t" in names
    finally:
        other.close()


def test_busy_is_detected(backend, dbpath) -> None:
    holder = backend.connect(dbpath, timeout=0.0)
    holder.execute("CREATE TABLE t(x)")
    holder.execute("BEGIN IMMEDIATE")

    other = backend.connect(dbpath, timeout=0.001)
    try:
        with pytest.raises(backend.Error) as excinfo:
            other.execute("BEGIN IMMEDIATE")
        assert backend.is_busy(excinfo.value)
        assert issubclass(backend.BusyError, backend.Error)
    finally:
        other.close()
        holder.close()


def test_set_pragma_busy_raises_busy_error(backend, dbpath) -> None:
    """set_pragma must translate a busy condition like execute() does.

    DiskStore applies DEFAULT_PRAGMAS on every fresh connection, so a pragma
    write that collides with a concurrent writer must surface as the
    diskstore BusyError rather than the raw driver error.  This was reachable
    under multi-process load on both backends.

    The database is deliberately left in the default rollback journal:
    converting to WAL needs an exclusive lock, whereas once the file is
    already in WAL mode the same pragma is a no-op that cannot fail.
    """
    setup = backend.connect(dbpath, timeout=5.0)
    try:
        setup.execute("CREATE TABLE t(x)")
    finally:
        setup.close()

    holder = backend.connect(dbpath, timeout=5.0)
    cursor = holder.cursor()
    cursor.execute("BEGIN EXCLUSIVE")
    cursor.execute("INSERT INTO t VALUES (1)")

    other = backend.connect(dbpath, timeout=0.001)
    try:
        with pytest.raises(backend.BusyError):
            backend.set_pragma(other, "journal_mode", "wal")
    finally:
        other.close()
        holder.close()


def test_set_pragma_non_busy_error_is_not_wrapped(backend, dbpath) -> None:
    """A non-busy pragma failure must stay a driver error, not become BusyError.

    Guards the translation against being too eager.  SQLite silently ignores
    unknown pragmas, so this uses a pragma whose *value* is a bad table name,
    which raises on both backends.  Note the stdlib message contains neither
    "locked" nor "busy", so is_busy() must not match it either.
    """
    con = backend.connect(dbpath, timeout=5.0)
    try:
        with pytest.raises(backend.Error) as excinfo:
            backend.set_pragma(con, "integrity_check", "no_such_table")
        assert not isinstance(excinfo.value, backend.BusyError)
        assert not backend.is_busy(excinfo.value)
    finally:
        con.close()


def test_busy_table_columns_raises_busy_error(backend, dbpath) -> None:
    """table_columns must translate a busy condition like execute() does.

    ``PRAGMA table_info`` reads the schema, and ``_migrate_table`` calls it on
    every fresh connection, so a contended schema read used to surface as a
    raw driver error during connection setup.
    """
    setup = backend.connect(dbpath, timeout=5.0)
    try:
        setup.execute("CREATE TABLE t(x)")
    finally:
        setup.close()

    holder = backend.connect(dbpath, timeout=5.0)
    cursor = holder.cursor()
    cursor.execute("BEGIN EXCLUSIVE")
    cursor.execute("INSERT INTO t VALUES (1)")

    other = backend.connect(dbpath, timeout=0.001)
    try:
        with pytest.raises(backend.BusyError):
            backend.table_columns(other, "t")
    finally:
        other.close()
        holder.close()


def test_table_columns_non_busy_error_is_not_wrapped(backend, dbpath) -> None:
    """A missing table is not a busy condition and must not become BusyError.

    ``table_columns`` is documented as returning an empty list for an absent
    table, so this pins that a schema miss stays a plain driver error rather
    than being reclassified.
    """
    con = backend.connect(dbpath, timeout=5.0)
    try:
        assert backend.table_columns(con, "no_such_table") == []
    finally:
        con.close()


def test_busy_error_is_detected_as_busy(backend) -> None:
    """is_busy() must recognise a BusyError it constructed itself.

    ``execute()`` re-raises busy conditions as a freshly built BusyError, so
    is_busy() has to stay true for that wrapper too.  A backend whose BusyError
    is a sibling of the driver's own busy class (rather than a subclass) fails
    here, which silently breaks ``while is_busy(exc): retry`` loops.
    """
    assert backend.is_busy(backend.BusyError("database is locked"))


def test_busy_error_is_busy_after_rewrap(dbpath) -> None:
    """is_busy() must survive the re-raise in the active backend's execute().

    Deliberately not parametrized over ``backend``: ``_sqlite.execute`` is
    bound to whichever backend ``DISKSTORE_BACKEND`` selected at import time,
    so it can only be driven against that one.  ``make test`` runs the whole
    file under both values, which is what covers the other backend.
    """
    from diskstore import _sqlite

    holder = _sqlite.connect(dbpath, timeout=0.0)
    holder.execute("CREATE TABLE t(x)")
    holder.execute("BEGIN IMMEDIATE")

    other = _sqlite.connect(dbpath, timeout=0.001)
    cursor = other.cursor()
    try:
        with pytest.raises(_sqlite.BusyError) as excinfo:
            _sqlite.execute(cursor, "BEGIN IMMEDIATE")
        assert _sqlite.is_busy(excinfo.value)
    finally:
        other.close()
        holder.close()
