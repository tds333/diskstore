"""SQLite driver backend using APSW, when it is installed.

Implements the backend interface consumed by ``diskstore._sqlite``.
APSW bundles its own recent SQLite, so no version check is needed.
"""

import os
from typing import Any

import apsw

Connection = apsw.Connection
Cursor = apsw.Cursor
SQLError = apsw.SQLError
Error = apsw.Error


class BusyError(apsw.Error):
    """Raised when SQLite reports a busy condition."""


def is_busy(exc: Exception) -> bool:
    """Whether *exc* is an APSW busy error."""
    return isinstance(exc, apsw.BusyError)


def connect(
    filename: os.PathLike | str,
    *,
    readonly: bool = False,
    timeout: float = 10.0,
) -> Connection:
    """Open an APSW connection, read-only when requested.

    APSW is always in autocommit until an explicit ``BEGIN``, matching the
    transaction handling in ``DiskStore.transact``.
    """
    path = os.fsdecode(os.fspath(filename))
    if readonly:
        con = apsw.Connection(path, flags=apsw.SQLITE_OPEN_READONLY)
    else:
        con = apsw.Connection(path)
    con.set_busy_timeout(int(timeout * 1000))
    return con


def set_pragma(con: Connection, key: str, value: Any) -> None:
    """Set PRAGMA *key* to *value* on *con*."""
    con.pragma(key, value)


def get_pragma(con: Connection, key: str) -> Any:
    """Return the first column of ``PRAGMA key`` (``None`` if empty)."""
    return con.pragma(key)


def table_columns(con: Connection, table: str) -> list[tuple]:
    """Return ``PRAGMA table_info`` rows for *table* (empty if absent)."""
    rows = con.pragma("table_info", table)
    return list(rows) if rows is not None else []
