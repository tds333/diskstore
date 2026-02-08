"""DiskRead (Mapping) API."""

import os
import os.path
import threading
from collections.abc import ItemsView, KeysView, Mapping, ValuesView
from contextlib import closing, contextmanager
from typing import Generator, NamedTuple, Optional, Sequence, TypeAlias, Union

import apsw

from .const import MISSING, TIMEOUT, KeyType

Connection = apsw.Connection
Cursor = apsw.Cursor


BasicType: TypeAlias = Union[bytes, str, int, float]


class Value(NamedTuple):
    value: BasicType

    def __float__(self):
        data = self[0]
        if isinstance(data, float):
            return data
        raise ValueError("Internal value is not an instance of float.")

    def __int__(self):
        data = self[0]
        if isinstance(data, int):
            return data
        raise ValueError("Internal value is not an instance of int.")

    def __bytes__(self):
        data = self[0]
        if isinstance(data, bytes):
            return data
        raise ValueError("Internal value is not an instance of bytes.")

    def __str__(self):
        return str(self[0])


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
    """SQLite read only disk storage. Does not connect on init!"""

    def __init__(  # noqa: PLR0915
        self,
        filename: os.PathLike | str,
        value_class=None,
        tablename: str | None = None,
        timeout=None,
    ) -> None:
        """Initialize cache instance.

        :param str filename: filename for DB to use.
        :param str tablename: Optional table name to use.
        :param type value_class: Class type (inherited from NamedTuple)
                                 used to get back data.
        :param float timeout: SQLite connection timeout
                              (also used if DB is busy to retry)
        """
        filename = os.path.expanduser(filename)
        filename = os.path.expandvars(filename)
        if value_class is None:
            self._value_class = Value
        else:
            self._value_class = value_class
        self._tablename = (
            self._value_class.__name__
            if tablename is None or tablename == "_Settings"
            else tablename
        )

        self._filename = os.fspath(filename)
        self._timeout: float = TIMEOUT if timeout is None else float(timeout)
        self._local = threading.local()

        # precreated statements based on tablename and value_class
        tablename = self._escape_table_name(self._tablename)
        value_columns = self.get_fields(self._value_class)
        fields = ", ".join(f"{field}" for field in value_columns)
        self._statements: dict[str, str] = {
            "GET": f"SELECT {fields} FROM {tablename} WHERE _key = ? LIMIT 1",
            "CONTAINS": f"SELECT _key FROM {tablename} WHERE _key = ? LIMIT 1",
            "ITER": f"SELECT _key FROM {tablename} ORDER BY rowid ASC",
            "REVERSED": f"SELECT _key FROM {tablename} ORDER BY rowid DESC",
            "COUNT": f"SELECT COUNT (_key) FROM {tablename}",
            "QUERY": f"SELECT _key, {fields} FROM {tablename}",
        }

    @staticmethod
    def get_fields(value_class):
        value_columns = tuple(value_class._fields)
        if "_key" in value_columns:
            raise ValueError(
                f"Name _key is not allowed as attribute for {value_class},"
                " listed as field name in _fields."
            )

        return value_columns

    @staticmethod
    def _escape_table_name(name: str) -> str:
        tablename = '"' + name.replace('"', '""') + '"'
        return tablename

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
        return self._tablename

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
        select = self._statements["GET"]
        with self._cursor() as cursor:
            row = next(cursor.execute(select, (key,)), MISSING)

        if row is MISSING:
            raise KeyError(key)

        return self._value_class(*row)  # ty:ignore[not-iterable]

    def keys(self):
        return DiskKeysView(self)

    def values(self):
        return DiskValuesView(self)

    def items(self):
        return DiskItemsView(self)

    def query(
        self,
        where: Optional[str] = None,
        parameters: Optional[Sequence] = None,
        order: Optional[str] = None,
    ) -> Generator[tuple, None, None]:
        where = " WHERE " + where if where else ""
        parameters = () if parameters is None else parameters
        order = " ORDER BY " + order if order else ""
        select = self._statements["QUERY"] + where + order

        with self._cursor() as cursor:
            cursor.execute(select, parameters)
            for row in cursor:
                yield row[0], self._value_class(*row[1:])

    def __contains__(self, key: object) -> bool:
        with self._cursor() as cx:
            rows = cx.execute(self._statements["CONTAINS"], (key,)).fetchall()

        return bool(rows)

    def __iter__(self):
        with self._cursor() as cx:
            cx.execute(self._statements["ITER"])
            for row in cx:
                yield row[0]

    def __reversed__(self):
        with self._cursor() as cx:
            cx.execute(self._statements["REVERSED"])
            for row in cx:
                yield row[0]

    def close(self) -> None:
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
        select = self._statements["COUNT"]

        with closing(self._con.execute(select)) as cx:
            rows = next(cx, (0,))
        return rows[0]

    def __getstate__(self):
        return {
            "filename": self.filename,
            "timeout": self.timeout,
            "tablename": self._tablename,
            "value_class": self._value_class,
        }

    def __setstate__(self, state) -> None:
        self.__init__(**state)
