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
import weakref
from collections.abc import ItemsView, KeysView, Mapping, ValuesView
from contextlib import closing
from typing import Generator, Optional, Sequence, TypeAlias, Union

from ._sqlite import Connection, Cursor, connect, set_pragma
from .config import BaseConfig, ConfigProtocol, escape_name
from .const import DEFAULT_RO_PRAGMAS, TIMEOUT, KeyType

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


class _Holder:
    """A live connection held weakly by the store's registry.

    ``sqlite3.Connection`` objects cannot be weak-referenced, so the store
    registers these holders in a :class:`weakref.WeakSet` instead.  When a
    thread's local state is collected the holder (and with it the connection)
    is reclaimed exactly as before, so the registry never keeps an abandoned
    thread's file descriptor alive.
    """

    __slots__ = ("__weakref__", "con")

    def __init__(self, con: Connection) -> None:
        self.con = con


class _ThreadState(threading.local):
    """Per-thread connection state.

    The class-level defaults are visible from every thread, unlike
    attributes assigned in ``__init__`` which only apply to the constructing
    thread.  Callers can therefore read the fields directly without
    ``getattr``.
    """

    con: Connection | None = None
    cursor: Cursor | None = None
    # The fork token and the store's close generation summed into one value.
    # Both only ever increase, so a change in either -- a fork, or a
    # ``close_all()`` -- makes the sum differ and the connection is rebuilt.
    generation: int | None = None
    in_transaction: bool = False
    holder: _Holder | None = None


class DiskKeysView(KeysView):
    __slots__ = ()

    def __iter__(self):
        return iter(self._mapping)

    def __reversed__(self):
        return reversed(self._mapping)


class DiskValuesView(ValuesView):
    __slots__ = ()

    def __iter__(self):
        for _, value in self._mapping.query(order="rowid ASC"):
            yield value

    def __reversed__(self):
        for _, value in self._mapping.query(order="rowid DESC"):
            yield value


class DiskItemsView(ItemsView):
    __slots__ = ()

    def __iter__(self):
        return self._mapping.query(order="rowid ASC")

    def __reversed__(self):
        return self._mapping.query(order="rowid DESC")


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

        # Registry of live connections so ``close_all()`` can reach every
        # thread's connection.  ``_close_generation`` counts ``close_all()``
        # calls; added to the global fork token it forms the generation each
        # thread's cached connection is stamped with, so either a fork or a
        # ``close_all()`` makes that thread rebuild its connection.
        self._holders: weakref.WeakSet[_Holder] = weakref.WeakSet()
        self._lock = threading.Lock()
        self._close_generation = 0

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
        # A change in the fork token or the close generation (a fork, or a
        # close_all()) invalidates the cached connection, which is dropped and
        # recreated.

        local = self._local
        generation = _fork_token() + self._close_generation

        if local.generation != generation:
            self.close()
            local.generation = generation

        con = local.con

        if con is None:
            con = connect(self._filename, readonly=True, timeout=self._timeout)

            # Some SQLite pragmas work on a per-connection basis so
            # apply them all on fresh connection
            for key, value in self._pragmas.items():
                set_pragma(con, key, value)

            holder = _Holder(con)
            local.con = con
            local.holder = holder
            with self._lock:
                self._holders.add(holder)

        return con

    @property
    def _cursor(self) -> Cursor:
        # Reuse a single cursor per thread for single-shot operations to
        # avoid a cursor allocation per call. Iteration and transact() use
        # their own cursors so nested reads stay safe.  Fast path avoids the
        # _con property; the slow path (fresh thread, fork or close_all())
        # creates it.
        local = self._local
        cursor = local.cursor
        if cursor is None or local.generation != _fork_token() + self._close_generation:
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
        """Close this thread's database connection if open."""
        local = self._local
        con = local.con
        if con is None:
            return
        con.close()
        local.con = None
        local.cursor = None
        holder = local.holder
        if holder is not None:
            with self._lock:
                self._holders.discard(holder)
            local.holder = None

    def close_all(self) -> int:
        """Close every open connection, including those of other threads.

        Connections opened by the calling thread are always closed.  Other
        threads' connections are closed too: each connection is created with
        SQLite's cross-thread check disabled (or by APSW, which is
        thread-safe), so it can be closed from a different thread than the one
        that opened it.

        Any thread that touches the store afterwards transparently rebuilds
        its own connection, exactly as after :meth:`close`.

        Only call this when the other threads are not mid-operation; closing a
        connection another thread is actively using is still a race.  It is
        intended for teardown, when the store is no longer being used.

        Returns the number of connections closed.
        """
        with self._lock:
            # Bump the close generation so every thread rebuilds its connection
            # on next use, then snapshot the holders that exist right now.
            self._close_generation += 1
            holders = list(self._holders)

        closed = 0
        for holder in holders:
            holder.con.close()
            with self._lock:
                self._holders.discard(holder)
            closed += 1

        # Forget this thread's local state as well.
        local = self._local
        local.con = None
        local.cursor = None
        local.holder = None
        return closed

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
