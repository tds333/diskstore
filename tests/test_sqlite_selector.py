"""Tests for backend selection and the shared selector API."""

import importlib.util
import os
import subprocess
import sys
import tempfile

import pytest

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
