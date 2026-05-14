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
from contextlib import closing, contextmanager
from typing import Generator, Optional, Sequence, TypeAlias, Union

import apsw

from .config import BaseConfig, ConfigProtocol, escape_name
from .const import MISSING, TIMEOUT, KeyType

Connection = apsw.Connection
Cursor = apsw.Cursor


BasicType: TypeAlias = Union[bytes, str, int, float]


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
        filename = os.path.expandvars(filename)
        self._filename = os.fspath(filename)
        self._config: ConfigProtocol = BaseConfig() if config is None else config
        self._timeout: float = (
            TIMEOUT
            if (self._config.timeout is None or self._config.timeout < 0.0)
            else float(self._config.timeout)
        )
        self._load_data = self._config.load_data
        self._local = threading.local()

        # precreated statements based on tablename and value_class
        tablename = escape_name(self._config.tablename)
        fields = ", ".join(f"{escape_name(field)}" for field, *_ in self._config.fields)
        self._statements: dict[str, str] = {
            "GET": f"SELECT {fields} FROM {tablename} WHERE _key = ? LIMIT 1",
            "CONTAINS": f"SELECT _key FROM {tablename} WHERE _key = ? LIMIT 1",
            "ITER": f"SELECT _key FROM {tablename} ORDER BY rowid ASC",
            "REVERSED": f"SELECT _key FROM {tablename} ORDER BY rowid DESC",
            "COUNT": f"SELECT COUNT (_key) FROM {tablename}",
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
        # Check process ID to support process forking. If the process
        # ID changes, close the connection and update the process ID.

        local_pid = getattr(self._local, "pid", None)
        pid: int = os.getpid()

        if local_pid != pid:
            self.close()
            self._local.pid: int = pid

        con = getattr(self._local, "con", None)

        if con is None:
            con = self._local.con = Connection(
                self._filename, flags=apsw.SQLITE_OPEN_READONLY
            )
            con.set_busy_timeout(int(self._timeout * 1000))

        return con

    @contextmanager
    def _cursor(self):
        cursor: Cursor = self._con.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def __getitem__(self, key: KeyType):
        """Get value for *key*, raises KeyError if not found."""
        select = self._statements["GET"]
        with self._cursor() as cursor:
            row = next(cursor.execute(select, (key,)), MISSING)

        if row is MISSING:
            raise KeyError(key)

        return self._load_data(row)  # ty:ignore[invalid-argument-type]

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
        limit_ = " LIMIT " + str(limit) if limit is not None else ""
        offset_ = " OFFSET " + str(offset) if offset is not None else ""
        select = self._statements["QUERY"] + where_ + order_ + limit_ + offset_

        with self._cursor() as cursor:
            cursor.execute(select, parameters_)
            for row in cursor:
                yield row[0], self._load_data(row[1:])

    def __contains__(self, key: object) -> bool:
        """Check if *key* exists in the store."""
        with self._cursor() as cx:
            rows = cx.execute(self._statements["CONTAINS"], (key,)).fetchall()

        return bool(rows)

    def __iter__(self):
        """Iterate over keys in insertion order."""
        with self._cursor() as cx:
            cx.execute(self._statements["ITER"])
            for row in cx:
                yield row[0]

    def __reversed__(self):
        """Iterate over keys in reverse insertion order."""
        with self._cursor() as cx:
            cx.execute(self._statements["REVERSED"])
            for row in cx:
                yield row[0]

    def open(self):
        """Open (or re-open) the database connection and return self."""
        connection = self._con  # noqa
        return self

    def close(self) -> None:
        """Close the database connection if open."""
        con = getattr(self._local, "con", None)
        if con is None:
            return
        con.close()
        try:
            delattr(self._local, "con")
        except AttributeError:
            pass

    def __enter__(self):
        connection = self._con  # noqa
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.close()

    def __len__(self):
        """Return the number of items in the store.

        To do this count is used which is not a performant implementation.
        """
        select = self._statements["COUNT"]

        with closing(self._con.execute(select)) as cx:
            rows = next(cx, (0,))
        return rows[0]

    def __getstate__(self):
        return {
            "filename": self.filename,
            "config": self._config,
        }

    def __setstate__(self, state) -> None:
        self.__init__(**state)
