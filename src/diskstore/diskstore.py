"""DiskStore (MutableMapping) API."""

import os
import os.path
import threading
from collections.abc import Mapping, MutableMapping
from contextlib import closing, contextmanager
from time import sleep, time
from typing import Iterable

import apsw

from .const import DEFAULT_PRAGMAS, MISSING, TIMEOUT, KeyType
from .diskread import DiskRead

Connection = apsw.Connection
Cursor = apsw.Cursor
SQLError = apsw.SQLError
BusyError = apsw.BusyError


class DiskStore(DiskRead, MutableMapping):
    """SQLite disk storage."""

    def __init__(  # noqa: PLR0915
        self,
        filename: os.PathLike | str,
        value_class=None,
        tablename: str | None = None,
        key_type=None,
        timeout: float | None = None,
        alter_table: bool = True,
        **pragmas,
    ) -> None:
        """Initialize Diskstore instance.

        :param str filename: DiskStore DB filename.
                              If not set a tmp directory and default DB name is created.
        :param str tablename: Optional table name to use.
                              If not set the name from value_class is used.
        :param type value_class: Class type (inherited from NamedTuple)
                                 used to get back data. Default is Value class.
        :param float timeout: SQLite connection timeout
                              (also used if DB is busy to retry)
        :param pragmas: Any of DEFAULT_PRAGMAS or additional PRAGMA's to apply
        """
        super().__init__(
            filename=filename,
            value_class=value_class,
            tablename=tablename,
            timeout=timeout,
        )
        self._txn_id = None
        tablename = self._escape_table_name(self._tablename)

        primary_key_type = "BLOB"
        if key_type is str:
            primary_key_type = "TEXT"
        elif key_type is int:
            primary_key_type = "INTEGER"
        elif key_type is float:
            primary_key_type = "REAL"
        value_columns = self.get_fields(self._value_class)
        column_types = self.get_field_types(self._value_class)
        fields_create = ", ".join(
            f"{field} {ctype} NOT NULL"
            for field, ctype in zip(value_columns, column_types, strict=True)
        )
        fields = ", ".join(f"{field}" for field in value_columns)
        excluded_fields = ", ".join(
            f"{field} = excluded.{field}" for field in value_columns
        )
        qms = ", ".join("?" for field in value_columns)
        self._statements.update(
            {
                "CREATE": (
                    f"CREATE TABLE IF NOT EXISTS {tablename} ("
                    f" _key {primary_key_type} PRIMARY KEY NOT NULL"
                    f", {fields_create})"
                ),
                "SET": (
                    f"INSERT INTO {tablename}(_key, {fields}) VALUES (?, {qms})"
                    f" ON CONFLICT (_key) DO UPDATE SET {excluded_fields}"
                ),
                "ADD": (
                    f"INSERT INTO {tablename}(_key, {fields}) "
                    f"VALUES (?, {qms}) ON CONFLICT DO NOTHING RETURNING _key"
                ),
                "DELETE": f"DELETE FROM {tablename} WHERE _key = ? RETURNING _key",
                "CLEAR": f"DELETE FROM {tablename};VACUUM;",
                "POPITEM": (
                    f"SELECT _key, {fields} FROM {tablename} ORDER BY"
                    " rowid DESC LIMIT 1"
                ),
            }
        )
        self._pragmas = DEFAULT_PRAGMAS.copy()
        self._pragmas.update(pragmas)
        sql = self._con.execute
        # Setup table.
        with closing(sql(self._statements["CREATE"])):
            pass
        # try alter table if needed
        if alter_table:
            self._alter_table(sql)

    @staticmethod
    def get_field_types(value_class):
        value_columns = tuple(value_class._fields)
        if hasattr(value_class, "__annotations__") and value_class.__annotations__:
            type_annotations = value_class.__annotations__
            value_types = []
            for c_name in value_columns:
                type_cls = type_annotations.get(c_name, bytes)  # types are optional
                if type_cls is str:
                    value_types.append("TEXT")
                elif type_cls is int:
                    value_types.append("INTEGER")
                elif type_cls is float:
                    value_types.append("REAL")
                else:
                    value_types.append("BLOB")
        else:
            value_types = ["BLOB" for _ in value_columns]

        return value_types

    def _validate_value(self, value):
        if isinstance(value, self._value_class):
            return value
        value_type = type(value)
        raise ValueError(
            f"Value must be instance of {self._value_class} but is {value_type}."
        )

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
            con = self._local.con = Connection(self._filename)
            con.set_busy_timeout(int(self._timeout * 1000))

            # Some SQLite pragmas work on a per-connection basis so
            # apply them all on fresh connection
            for key, value in self._pragmas.items():
                con.pragma(key, value)

        return con

    def _alter_table(self, sql):
        value_columns = self.get_fields(self._value_class)
        column_types = self.get_field_types(self._value_class)
        tablename = self._escape_table_name(self._tablename)
        value_column_defaults = getattr(self._value_class, "_field_defaults", {})
        existing_fileds = set()
        table_info_stmt = f"PRAGMA table_info({tablename});"
        with closing(sql(table_info_stmt)) as cursor:
            for row in cursor:
                existing_fileds.add(row[1])
        for columnname, columntype in zip(value_columns, column_types, strict=True):
            if columnname not in existing_fileds:
                default = apsw.format_sql_value(value_column_defaults[columnname])
                alter_stmt = (
                    f"ALTER TABLE {tablename} ADD COLUMN {columnname}"
                    f" {columntype} NOT NULL DEFAULT {default};"
                )
                with closing(sql(alter_stmt)) as cursor:
                    pass

    @contextmanager
    def transact(self, retry=False):
        cursor: Cursor = self._con.cursor()
        tid = threading.get_ident()
        txn_id = self._txn_id
        retry_until = time() + TIMEOUT

        if tid == txn_id:  # already inside a thread with a transaction
            begin = False
        else:
            while True:
                try:
                    cursor.execute("BEGIN IMMEDIATE")
                    begin = True
                    self._txn_id = tid
                    break
                except BusyError:
                    if retry and time() < retry_until:
                        sleep(0.001)
                        continue
                    raise

        try:
            yield cursor
        except BaseException:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                cursor.execute("ROLLBACK")
            cursor.close()
            raise
        else:
            if begin:
                assert self._txn_id == tid
                self._txn_id = None
                cursor.execute("COMMIT")
            cursor.close()

    def __setitem__(self, key: KeyType, value: Iterable) -> None:
        value = self._validate_value(value)
        with self._cursor() as cursor:
            cursor.execute(self._statements["SET"], (key, *value))

    def add(self, key: KeyType | None, value: Iterable) -> KeyType | None:
        value = self._validate_value(value)
        with self._cursor() as cx:
            rows = cx.execute(self._statements["ADD"], (key, *value)).fetchall()

        if not rows:
            return None

        return rows[0][0]

    def pop(self, key: KeyType, default=MISSING):  # noqa: E501
        with self.transact(retry=True) as cx:
            value = next(cx.execute(self._statements["GET"], (key,)), None)
            if value is None:
                if default is MISSING:
                    raise KeyError(key)
                return default
            cx.execute(self._statements["DELETE"], (key,))
            return self._value_class(*value)

    def popitem(self):
        with self.transact(retry=True) as cx:
            row = next(cx.execute(self._statements["POPITEM"]), None)
            if not row:
                raise KeyError()
            key = row[0]
            value = self._value_class(*row[1:])
            cx.execute(self._statements["DELETE"], (key,))
            return key, value

    def __delitem__(self, key: KeyType) -> None:
        with self._cursor() as cursor:
            rows = cursor.execute(self._statements["DELETE"], (key,)).fetchall()
        if not rows:
            raise KeyError(key)

    def setdefault(self, key: KeyType, default: Iterable | None = None):
        try:
            return self[key]
        except KeyError:
            if default is not None:
                default = self._validate_value(default)
                self.add(key, default)

        return default

    def check(self, vacuum=False):
        warns = []
        sql = self._con.execute

        # Check integrity of database.
        with closing(sql("PRAGMA integrity_check")) as cx:
            rows = cx.fetchall()

        if len(rows) != 1 or rows[0][0] != "ok":
            for (message,) in rows:
                warns.append(message)

        if vacuum:
            with closing(sql("VACUUM")):
                pass

        return warns

    def clear(self) -> None:
        with closing(self._con.execute(self._statements["CLEAR"])):
            pass

    def update(self, other=(), /, **kwargs):  # type: ignore
        comps = []
        if other:
            if isinstance(other, Mapping):
                comp = ((key, *value) for key, value in other.items())
                comps.append(comp)
            elif hasattr(other, "keys"):
                comp = ((key, *other[key]) for key in other.keys())
                comps.append(comp)
            else:
                comp = ((key, *value) for key, value in other)
                comps.append(comp)
        if kwargs:
            comp = ((key, *value) for key, value in kwargs.items())
            comps.append(comp)

        with self.transact(retry=True) as cursor:
            for comp in comps:
                cursor.executemany(self._statements["SET"], comp)

    def get_readonly_instance(self):
        return DiskRead(
            self._filename, self._value_class, self._tablename, self._timeout
        )
