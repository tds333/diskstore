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
