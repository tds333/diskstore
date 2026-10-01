"""Tests for backend selection and the shared selector API."""

import builtins
import importlib
import importlib.util
import os
import subprocess
import sys
import tempfile

import pytest

import diskstore
from diskstore import _sqlite

HAVE_APSW = importlib.util.find_spec("apsw") is not None
EXPECTED_DEFAULT = "apsw" if HAVE_APSW else "sqlite3"


def _run(env_value):
    env = dict(os.environ)
    env.pop("DISKSTORE_BACKEND", None)
    if env_value is not None:
        env["DISKSTORE_BACKEND"] = env_value
    code = "from diskstore import _sqlite; print(_sqlite.BACKEND_NAME)"
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_backend_name_is_known() -> None:
    assert _sqlite.BACKEND_NAME in {"apsw", "sqlite3"}


def test_forced_sqlite3() -> None:
    result = _run("sqlite3")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "sqlite3"


@pytest.mark.skipif(not HAVE_APSW, reason="apsw not installed")
def test_forced_apsw() -> None:
    result = _run("apsw")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "apsw"


def test_default_prefers_apsw_when_available() -> None:
    result = _run(None)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == EXPECTED_DEFAULT


def test_invalid_backend_is_rejected() -> None:
    result = _run("nope")
    assert result.returncode != 0
    assert "DISKSTORE_BACKEND" in result.stderr


@pytest.fixture
def restore_selector():
    """Restore the module-level selection state after a reload.

    ``_sqlite`` binds its backend at import time, so exercising the other
    branches means reloading it.  Both the environment variable and
    ``builtins.__import__`` must be put back before the reload, otherwise
    later tests inherit a half-restored module.
    """
    saved_env = os.environ.get("DISKSTORE_BACKEND")
    saved_import = builtins.__import__
    yield
    builtins.__import__ = saved_import
    if saved_env is None:
        os.environ.pop("DISKSTORE_BACKEND", None)
    else:
        os.environ["DISKSTORE_BACKEND"] = saved_env
    importlib.reload(_sqlite)


def test_auto_prefers_apsw_when_importable(monkeypatch, restore_selector) -> None:
    monkeypatch.delenv("DISKSTORE_BACKEND", raising=False)
    importlib.reload(_sqlite)
    assert _sqlite.BACKEND_NAME == EXPECTED_DEFAULT


def test_auto_falls_back_to_stdlib(monkeypatch, restore_selector) -> None:
    monkeypatch.delenv("DISKSTORE_BACKEND", raising=False)
    # The cached submodule and the parent-package attribute both have to go;
    # removing only one lets the import machinery find apsw anyway.
    monkeypatch.delitem(sys.modules, "diskstore._sqlite_apsw", raising=False)
    monkeypatch.delattr(diskstore, "_sqlite_apsw", raising=False)

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if "apsw" in name:
            raise ImportError("apsw blocked for test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    importlib.reload(_sqlite)
    assert _sqlite.BACKEND_NAME == "sqlite3"


def test_invalid_backend_raises_on_reload(monkeypatch, restore_selector) -> None:
    monkeypatch.setenv("DISKSTORE_BACKEND", "nope")
    with pytest.raises(ValueError, match="DISKSTORE_BACKEND"):
        importlib.reload(_sqlite)


def test_selector_exports_common_helpers() -> None:
    assert _sqlite.format_sql_value is _sqlite.quote_literal
    names = (
        "BusyError",
        "Connection",
        "Cursor",
        "SQLError",
        "Error",
        "connect",
        "set_pragma",
        "get_pragma",
        "table_columns",
        "is_busy",
        "execute",
        "executemany",
    )
    for name in names:
        assert hasattr(_sqlite, name), name


def test_selector_execute_translates() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
    holder = _sqlite.connect(dbpath, timeout=0.0)
    holder.execute("CREATE TABLE t(x)")
    holder.execute("BEGIN IMMEDIATE")
    other = _sqlite.connect(dbpath, timeout=0.001)
    try:
        with pytest.raises(_sqlite.BusyError):
            _sqlite.execute(other, "BEGIN IMMEDIATE")
    finally:
        other.close()
        holder.close()


def test_selector_executemany_translates() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
    holder = _sqlite.connect(dbpath, timeout=0.0)
    holder.execute("CREATE TABLE t(x)")
    holder.execute("BEGIN IMMEDIATE")
    other = _sqlite.connect(dbpath, timeout=0.001)
    try:
        with pytest.raises(_sqlite.BusyError):
            _sqlite.executemany(other, "INSERT INTO t VALUES (?)", [(1,)])
    finally:
        other.close()
        holder.close()


def test_selector_error_translation_leaves_other_errors_alone() -> None:
    """Non-busy errors must propagate unchanged, not be wrapped in BusyError."""
    with tempfile.NamedTemporaryFile(suffix=".db") as handle:
        dbpath = handle.name
    con = _sqlite.connect(dbpath)
    try:
        with pytest.raises(_sqlite.Error) as excinfo:
            _sqlite.execute(con, "SELECT * FROM missing_table")
        assert not isinstance(excinfo.value, _sqlite.BusyError)

        with pytest.raises(_sqlite.Error) as excinfo:
            _sqlite.executemany(con, "INSERT INTO missing_table VALUES (?)", [(1,)])
        assert not isinstance(excinfo.value, _sqlite.BusyError)
    finally:
        con.close()
