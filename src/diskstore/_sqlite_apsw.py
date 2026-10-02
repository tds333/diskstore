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

    The exception *type* is what this tests, not a code: APSW maps each SQLite
    result code to its own exception class, and ``apsw.BusyError`` is a
    sibling of ``apsw.SQLError`` under ``apsw.Error`` rather than a subclass
    of it, so an ``isinstance`` check against ``SQLError`` would miss busy.

    The code *is* recoverable from a raised error, as
    ``exc.extendedresult`` (or ``exc.result`` for the primary code), which is
    the counterpart to the stdlib backend's ``sqlite_errorname``.  Two
    behaviours worth knowing when retrying:

    - ``SQLITE_BUSY_SNAPSHOT`` (extended code 517) reports the same
      ``"database is locked"`` message as plain ``SQLITE_BUSY``, so the
      extended code is the only way to tell them apart.
    - ``SQLITE_BUSY_SNAPSHOT`` is returned directly rather than through the
      busy handler, so ``timeout`` is not honoured and the failure is
      immediate.  Retrying on a timer cannot help; the read snapshot has to
      be re-established for progress to be possible.
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
    """Set PRAGMA *key* to *value* on *con*.

    Busy conditions are translated to :class:`BusyError` as in
    ``_sqlite.execute``.  DiskStore applies its default pragmas on every
    fresh connection, so this is reachable whenever a new connection's
    pragma setup races another writer.
    """
    try:
        con.pragma(key, value)
    except apsw.Error as exc:
        if is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise


def get_pragma(con: Connection, key: str) -> Any:
    """Return the first column of ``PRAGMA key`` (``None`` if empty)."""
    return con.pragma(key)


def table_columns(con: Connection, table: str) -> list[tuple]:
    """Return ``PRAGMA table_info`` rows for *table* (empty if absent).

    Translates busy conditions to :class:`BusyError` like ``set_pragma``.
    ``PRAGMA table_info`` needs to read the schema, which can be blocked
    during connection setup on either backend.
    """
    try:
        rows = con.pragma("table_info", table)
    except apsw.Error as exc:
        if is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise
    return list(rows) if rows is not None else []
