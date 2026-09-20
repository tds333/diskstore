"""DiskRead (Mapping) API.

Examples:
    >>> from diskstore import DiskRead
    # DB must exist!
    >>> ds = DiskRead("/tmp/data.db")
    >>> ds["one"]
    1

"""

import os
import os.path
import threading
from collections.abc import ItemsView, KeysView, Mapping, ValuesView
from contextlib import closing
from typing import Generator, Optional, Sequence, TypeAlias, Union

import apsw

from .config import BaseConfig, ConfigProtocol, escape_name
from .const import DEFAULT_RO_PRAGMAS, TIMEOUT, KeyType

Connection = apsw.Connection
Cursor = apsw.Cursor

# Fork detection without calling os.getpid() on every operation.  On
# platforms with os.register_at_fork a generation counter is bumped in
# the child, otherwise fall back to comparing the process id.
_HAS_REGISTER_AT_FORK = hasattr(os, "register_at_fork")
_FORK_GENERATION = 0

if _HAS_REGISTER_AT_FORK:

    def _bump_fork_generation() -> None:
        global _FORK_GENERATION  # noqa: PLW0603
        _FORK_GENERATION += 1

    os.register_at_fork(after_in_child=_bump_fork_generation)


def _fork_token() -> int:
    return _FORK_GENERATION if _HAS_REGISTER_AT_FORK else os.getpid()


BasicType: TypeAlias = Union[bytes, str, int, float]


class _ThreadState(threading.local):
    """Per-thread connection state.

    The class-level defaults are visible from every thread, unlike
    attributes assigned in ``__init__`` which only apply to the constructing
    thread.  Callers can therefore read the fields directly without
    ``getattr``.
    """

    con: Connection | None = None
    cursor: Cursor | None = None
    fork_token: int | None = None
    in_transaction: bool = False


class DiskKeysView(KeysView):
    __slots__ = ()

    def __iter__(self):
        return iter(self._mapping)  # ty:ignore[unresolved-attribute]

    def __reversed__(self):
        return reversed(self._mapping)  # ty:ignore[unresolved-attribute]


class DiskValuesView(ValuesView):
    __slots__ = ()

    def __iter__(self):
        for _, value in self._mapping.query(order="rowid ASC"):  # ty:ignore[unresolved-attribute]
            yield value

    def __reversed__(self):
        for _, value in self._mapping.query(order="rowid DESC"):  # ty:ignore[unresolved-attribute]
            yield value


class DiskItemsView(ItemsView):
    __slots__ = ()

    def __iter__(self):
        return self._mapping.query(order="rowid ASC")  # ty:ignore[unresolved-attribute]

    def __reversed__(self):
        return self._mapping.query(order="rowid DESC")  # ty:ignore[unresolved-attribute]


class DiskRead(Mapping):
    def __init__(
        self,
        filename: os.PathLike | str,
        config: ConfigProtocol | None = None,
    ) -> None:
        """SQLite read only disk storage.

        Database is opened read only on demand.

        Args:
           filename: filename for DB to use.
           config: Configuration

        """
        filename = os.path.expanduser(filename)
        self._filename = os.fspath(filename)
        self._config: ConfigProtocol = BaseConfig() if config is None else config
        self._timeout: float = (
            TIMEOUT
            if (self._config.timeout is None or self._config.timeout < 0.0)
            else float(self._config.timeout)
        )
        self._load_data = self._config.load_data
        self._pragmas = DEFAULT_RO_PRAGMAS.copy()
        self._pragmas.update(self._config.pragmas)
        self._local: _ThreadState = _ThreadState()

        # precreated statements based on tablename and value_class
        tablename = escape_name(self._config.tablename)
        fields = ", ".join(f"{escape_name(field)}" for field, *_ in self._config.fields)
        self._statements: dict[str, str] = {
            "GET": f"SELECT _key, {fields} FROM {tablename} WHERE _key = ? LIMIT 1",
            "CONTAINS": f"SELECT 1 FROM {tablename} WHERE _key = ? LIMIT 1",
            "ITER": f"SELECT _key FROM {tablename} ORDER BY rowid ASC",
            "REVERSED": f"SELECT _key FROM {tablename} ORDER BY rowid DESC",
            "COUNT": f"SELECT COUNT(*) FROM {tablename}",
            "QUERY": f"SELECT _key, {fields} FROM {tablename}",
        }

    @property
    def filename(self) -> str:
        """DiskStore filename for DB."""
        return self._filename

    @property
    def timeout(self) -> float:
        """SQLite connection timeout value in seconds."""
        return self._timeout

    @property
    def tablename(self) -> str:
        """Tablename used to get data from."""
        return self._config.tablename

    @property
    def _con(self) -> Connection:
        # Detect process forking via a fork token. On a fork the token
        # changes, so the inherited connection is closed and recreated.

        local = self._local
        token = _fork_token()

        if local.fork_token != token:
            self.close()
            local.fork_token = token

        con = local.con

        if con is None:
            con = local.con = Connection(
                self._filename, flags=apsw.SQLITE_OPEN_READONLY
            )
            con.set_busy_timeout(int(self._timeout * 1000))

            # Some SQLite pragmas work on a per-connection basis so
            # apply them all on fresh connection
            for key, value in self._pragmas.items():
                con.pragma(key, value)

        return con

    @property
    def _cursor(self) -> Cursor:
        # Reuse a single cursor per thread for single-shot operations to
        # avoid a cursor allocation per call. Iteration and transact() use
        # their own cursors so nested reads stay safe.  Fast path avoids the
        # _con property; the slow path (fresh thread or fork) creates it.
        local = self._local
        cursor = local.cursor
        if cursor is None or local.fork_token != _fork_token():
            cursor = local.cursor = self._con.cursor()
        return cursor

    def __getitem__(self, key: KeyType):
        """Get value for *key*, raises KeyError if not found."""
        cursor = self._cursor
        cursor.execute(self._statements["GET"], (key,))
        # fetchall() drains the statement so it cannot block VACUUM.
        rows = cursor.fetchall()

        if not rows:
            raise KeyError(key)

        return self._load_data(rows[0])

    def keys(self):
        """Return a set-like view of keys in the mapping."""

        return DiskKeysView(self)

    def values(self):
        """Return a set-like view of values in the mapping."""

        return DiskValuesView(self)

    def items(self):
        """Return a set-like view of (key, value) pairs in the mapping."""

        return DiskItemsView(self)

    def query(
        self,
        where: Optional[str] = None,
        parameters: Optional[Sequence | dict] = None,
        order: Optional[str] = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Generator[tuple, None, None]:
        """Query rows with optional filtering, ordering, limit and offset.

        Args:
            where: SQL WHERE clause (without the WHERE keyword).
            parameters: Parameters for the WHERE clause.
            order: ORDER BY clause (without the ORDER BY keyword).
            limit: Maximum number of rows to return.
            offset: Number of rows to skip.

        Yields:
            (key, value) tuples.

        Examples:
            >>> from diskstore import DiskRead
            >>> ds = DiskRead("/tmp/data.db")
            >>> list(ds.query(where="_key > ?", parameters=(1,), limit=5))
            [(2, 'two'), (3, 'three')]

        """
        where_ = " WHERE " + where if where else ""
        parameters_ = () if parameters is None else parameters
        order_ = " ORDER BY " + order if order else ""
        limit_ = " LIMIT " + str(int(limit)) if limit is not None else ""
        offset_ = " OFFSET " + str(int(offset)) if offset is not None else ""
        select = self._statements["QUERY"] + where_ + order_ + limit_ + offset_

        with closing(self._con.execute(select, parameters_)) as cursor:
            for row in cursor:
                yield row[0], self._load_data(row)

    def __contains__(self, key: object) -> bool:
        """Check if *key* exists in the store."""
        cursor = self._cursor
        cursor.execute(self._statements["CONTAINS"], (key,))
        return bool(cursor.fetchall())

    def __iter__(self):
        """Iterate over keys in insertion order."""
        with closing(self._con.execute(self._statements["ITER"])) as cx:
            for row in cx:
                yield row[0]

    def __reversed__(self):
        """Iterate over keys in reverse insertion order."""
        with closing(self._con.execute(self._statements["REVERSED"])) as cx:
            for row in cx:
                yield row[0]

    def open(self) -> "DiskRead":
        """Open (or re-open) the database connection and return self."""
        connection = self._con  # noqa
        return self

    def close(self) -> None:
        """Close the database connection if open."""
        con = self._local.con
        if con is None:
            return
        con.close()
        self._local.con = None
        self._local.cursor = None

    def __enter__(self):
        connection = self._con  # noqa
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.close()

    def __len__(self):
        """Return the number of items in the store.

        ``COUNT(*)`` uses SQLite's specialised b-tree count instead of a
        row-by-row scan that evaluates the key column, which is
        substantially faster.
        """
        cursor = self._cursor
        cursor.execute(self._statements["COUNT"])
        rows = cursor.fetchall()
        return rows[0][0]

    def __getstate__(self):
        return {
            "filename": self.filename,
            "config": self._config,
        }

    def __setstate__(self, state) -> None:
        self.__init__(**state)
