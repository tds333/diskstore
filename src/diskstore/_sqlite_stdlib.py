"""SQLite driver backend using the standard-library
[sqlite3][sqlite3] module.

Implements the backend interface consumed by ``diskstore._sqlite``.
"""

import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from ._sqlite_common import _quote_identifier, quote_literal
from .const import TIMEOUT

SQLITE_MIN_VERSION = (3, 35)
"Minimum SQLite version required (``RETURNING`` support)."

Connection = sqlite3.Connection
Cursor = sqlite3.Cursor
SQLError = sqlite3.Error
Error = sqlite3.Error

_BUSY_NAMES = frozenset(
    {
        "SQLITE_BUSY",
        "SQLITE_BUSY_RECOVERY",
        "SQLITE_BUSY_SNAPSHOT",
        "SQLITE_BUSY_TIMEOUT",
    }
)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ClosingConnection(sqlite3.Connection):
    """A connection that closes itself on garbage collection.

    Per-thread connections are dropped without an explicit ``close`` when a
    thread ends (and doc examples are not always closed); closing in
    ``__del__`` releases the file handle deterministically instead of
    relying on the interpreter shutdown sequence, which emits a
    ``ResourceWarning``.
    """

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class BusyError(sqlite3.Error):
    """Raised when SQLite reports a busy condition.

    Derives from ``sqlite3.Error`` rather than ``sqlite3.OperationalError``
    to mirror the apsw backend, where ``apsw.BusyError`` sits directly under
    ``apsw.Error``.  Note that ``SQLError`` *is* ``sqlite3.Error`` on this
    backend, so a ``BusyError`` is unavoidably also a ``SQLError`` here while
    it is not one under apsw; catch ``(BusyError, SQLError)`` to get both
    backends' behaviour.
    """


def is_busy(exc: sqlite3.Error) -> bool:
    """Whether *exc* is a SQLite busy condition.

    Python 3.11+ exposes ``sqlite_errorname`` on raised errors; Python 3.10
    does not, so fall back to the message text there.
    """
    name = getattr(exc, "sqlite_errorname", None)
    if name is not None:
        return name in _BUSY_NAMES
    message = str(exc).lower()
    return "locked" in message or "busy" in message


def _readonly_uri(filename: os.PathLike | str) -> str:
    return Path(os.path.abspath(os.fspath(filename))).as_uri() + "?mode=ro"


def check_version() -> None:
    """Raise if the runtime SQLite is older than ``SQLITE_MIN_VERSION``."""
    if sqlite3.sqlite_version_info < SQLITE_MIN_VERSION:
        required = ".".join(map(str, SQLITE_MIN_VERSION))
        raise RuntimeError(
            f"diskstore requires SQLite >= {required}, found {sqlite3.sqlite_version}"
        )


def connect(
    filename: os.PathLike | str,
    *,
    readonly: bool = False,
    timeout: float = TIMEOUT,
) -> Connection:
    """Open an autocommit [sqlite3.Connection][sqlite3.Connection].

    ``isolation_level=None`` matches APSW's autocommit default so the
    explicit ``BEGIN IMMEDIATE`` handling in ``transact`` keeps working.
    """
    check_version()
    if readonly:
        return sqlite3.connect(
            _readonly_uri(filename),
            uri=True,
            timeout=timeout,
            isolation_level=None,
            factory=ClosingConnection,
        )
    return sqlite3.connect(
        filename, timeout=timeout, isolation_level=None, factory=ClosingConnection
    )


def _check_pragma_name(key: str) -> None:
    if not _IDENTIFIER.match(key):
        raise ValueError(f"invalid pragma name: {key!r}")


def set_pragma(con: Connection, key: str, value: Any) -> None:
    """Set PRAGMA *key* to *value* on *con*.

    SQLite does not accept bound parameters in PRAGMA statements, so the
    name is validated and the value quoted as a literal.

    Busy conditions are translated to :class:`BusyError` as in
    ``_sqlite.execute``.  DiskStore applies its default pragmas on every
    fresh connection, so this is reachable whenever a new connection's
    pragma setup races another writer.
    """
    _check_pragma_name(key)
    try:
        con.execute(f"PRAGMA {key}={quote_literal(value)}")
    except sqlite3.Error as exc:
        if is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise


def get_pragma(con: Connection, key: str) -> Any:
    """Return the first column of ``PRAGMA key`` (``None`` if empty)."""
    _check_pragma_name(key)
    row = con.execute(f"PRAGMA {key}").fetchone()
    return None if row is None else row[0]


def table_columns(con: Connection, table: str) -> list[tuple]:
    """Return ``PRAGMA table_info`` rows for *table*.

    Translates busy conditions to :class:`BusyError` like ``set_pragma``.
    ``PRAGMA table_info`` needs to read the schema, which can be blocked
    during connection setup on either backend.
    """
    try:
        return con.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    except sqlite3.Error as exc:
        if is_busy(exc):
            raise BusyError(str(exc)) from exc
        raise
