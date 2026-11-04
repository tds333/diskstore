"""SQLite backend selection.

Uses APSW when it is installed, otherwise the standard-library
[sqlite3][sqlite3] module.  Set the ``DISKSTORE_BACKEND`` environment variable
to ``apsw`` or ``sqlite3`` to force a backend (read at import time).

Both backends implement the same small interface (``connect``,
``set_pragma``, ``get_pragma``, ``table_columns``, ``is_busy``); the shared
busy translation lives here.
"""

import os
import random
import time
from collections.abc import Mapping
from typing import Any, Protocol, cast

from ._sqlite_common import (  # noqa: F401  (re-exported)
    format_sql_value,
    quote_literal,
)
from .const import TIMEOUT


class _Backend(Protocol):  # pragma: no cover
    """Interface implemented by each SQLite backend module."""

    Connection: Any
    Cursor: Any
    SQLError: Any
    Error: Any
    BusyError: Any

    def connect(
        self,
        filename: os.PathLike | str,
        *,
        readonly: bool = False,
        timeout: float = TIMEOUT,
    ) -> Any: ...

    def set_pragma(self, con: Any, key: str, value: Any) -> None: ...

    def get_pragma(self, con: Any, key: str) -> Any: ...

    def table_columns(self, con: Any, table: str) -> list[tuple]: ...

    def is_busy(self, exc: Exception) -> bool: ...


_BACKEND_ENV = "DISKSTORE_BACKEND"

_choice = os.environ.get(_BACKEND_ENV, "").strip().lower()
if _choice in ("", "auto"):
    try:
        from . import _sqlite_apsw as _backend

        BACKEND_NAME = "apsw"
    except ImportError:
        from . import _sqlite_stdlib as _backend

        BACKEND_NAME = "sqlite3"
elif _choice == "apsw":
    from . import _sqlite_apsw as _backend

    BACKEND_NAME = "apsw"
elif _choice in ("sqlite3", "stdlib"):
    from . import _sqlite_stdlib as _backend

    BACKEND_NAME = "sqlite3"
else:
    raise ValueError(
        f"unknown {_BACKEND_ENV}={_choice!r} (expected 'apsw' or 'sqlite3')"
    )

_impl: _Backend = cast("_Backend", _backend)

Connection = _impl.Connection
Cursor = _impl.Cursor
SQLError = _impl.SQLError
Error = _impl.Error
BusyError = _impl.BusyError
connect = _impl.connect
set_pragma = _impl.set_pragma
get_pragma = _impl.get_pragma
table_columns = _impl.table_columns
is_busy = _impl.is_busy

_RETRY_BASE_DELAY = 0.001
_RETRY_MAX_DELAY = 0.05


def apply_pragmas(
    con: Any, pragmas: Mapping[str, Any], *, timeout: float = TIMEOUT
) -> None:
    """Apply *pragmas* to *con*, retrying transient busy conditions.

    ``PRAGMA journal_mode=WAL`` upgrades a shared lock to an exclusive one.
    When two connections attempt that upgrade at the same time, SQLite
    deliberately skips the busy handler (waiting could deadlock) and returns
    ``SQLITE_BUSY`` immediately.  The failed statement releases its shared
    lock, so retrying lets one process win the conversion while the others
    find the database already in WAL mode.

    Retries stop once *timeout* seconds have elapsed; the per-attempt busy
    waits the drivers already perform are unchanged.  A zero timeout keeps
    the single-attempt behaviour.
    """
    deadline = time.monotonic() + timeout
    for key, value in pragmas.items():
        delay = _RETRY_BASE_DELAY
        while True:
            try:
                set_pragma(con, key, value)
                break
            except BusyError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(random.uniform(delay / 2, delay))
                delay = min(delay * 2, _RETRY_MAX_DELAY)


def execute(cursor: Cursor, sql: str, parameters: Any = ()) -> Cursor:
    """``cursor.execute`` translating busy conditions into ``BusyError``."""
    try:
        return cursor.execute(sql, parameters)
    except _impl.Error as exc:
        if _impl.is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise


def executemany(cursor: Cursor, sql: str, seq: Any) -> Cursor:
    """``cursor.executemany`` translating busy conditions into ``BusyError``."""
    try:
        return cursor.executemany(sql, seq)
    except _impl.Error as exc:
        if _impl.is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise
