"""SQLite driver backend using APSW, when it is installed.

Implements the backend interface consumed by ``diskstore._sqlite``.
APSW bundles its own recent SQLite, so no version check is needed.
"""

import os
from typing import Any

import apsw

from .const import TIMEOUT

Connection = apsw.Connection
Cursor = apsw.Cursor
SQLError = apsw.SQLError
Error = apsw.Error


class BusyError(apsw.BusyError):
    """Raised when SQLite reports a busy condition.

    Subclasses APSW's own ``BusyError`` so ``is_busy()`` keeps recognising
    the re-raised wrapper; ``apsw.BusyError`` is a sibling of
    ``apsw.SQLError``, not a subclass, so deriving from ``apsw.Error`` here
    would make a busy error invisible to ``is_busy()``.
    """


def is_busy(exc: Exception) -> bool:
    """Whether *exc* is an APSW busy error.

    The exception *type* is the only signal available here.  Unlike the stdlib
    backend, APSW exposes no error code on the raised error (no ``.code``, no
    ``.message``, and no module-level ``sqlite3_errstr``/``extended_errcode``
    to recover one), so the busy condition cannot be distinguished from any
    other by value.  Two consequences worth knowing:

    - ``SQLITE_BUSY_SNAPSHOT`` is indistinguishable from plain
      ``SQLITE_BUSY``; both report ``"database is locked"``.
    - ``SQLITE_BUSY_SNAPSHOT`` is returned directly rather than through the
      busy handler, so ``timeout`` is not honoured and the failure is
      immediate.  Retrying on a timer is the only option, and re-establishing
      the read snapshot is what actually makes progress.
    """
    return isinstance(exc, apsw.BusyError)


def connect(
    filename: os.PathLike | str,
    *,
    readonly: bool = False,
    timeout: float = TIMEOUT,
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
