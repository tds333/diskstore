"""DiskStore (MutableMapping) API.

Examples:
    >>> from diskstore import DiskStore
    >>> ds = DiskStore("/tmp/data.db")
    >>> ds["one"] = 1
    >>> ds["one"]
    1

"""

import os
import os.path
from collections.abc import Mapping, MutableMapping
from contextlib import closing, contextmanager
from typing import Any, Iterable

import apsw

from .config import ConfigProtocol, escape_name, get_sqlite_type
from .const import DEFAULT_PRAGMAS, MISSING, NO_DEFAULT, KeyType
from .diskread import DiskRead, _fork_token

Connection = apsw.Connection
Cursor = apsw.Cursor
SQLError = apsw.SQLError
BusyError = apsw.BusyError


class DiskStore(DiskRead, MutableMapping):
    def __init__(
        self, filename: os.PathLike | str, config: ConfigProtocol | None = None
    ) -> None:
        """SQLite based MutableMapping disk storage.

        Args:
            filename: DiskStore DB filename.
            config: Configuration as specified in ConfigProtocol
        """
        super().__init__(filename=filename, config=config)
        tablename = escape_name(self._config.tablename)

        value_columns = self._config.fields
        fields = ", ".join(f"{escape_name(field)}" for field, *_ in self._config.fields)
        excluded_fields = ", ".join(
            f"{field} = excluded.{escape_name(field)}"
            for field, *_ in self._config.fields
        )
        qms = ", ".join("?" for field in value_columns)
        self._statements.update(
            {
                "SET": (
                    f"INSERT INTO {tablename}(_key, {fields}) VALUES (?, {qms})"
                    f" ON CONFLICT (_key) DO UPDATE SET {excluded_fields}"
                ),
                "ADD": (
                    f"INSERT INTO {tablename}(_key, {fields}) "
                    f"VALUES (?, {qms}) ON CONFLICT DO NOTHING RETURNING _key"
                ),
                "DELETE": f"DELETE FROM {tablename} WHERE _key = ? RETURNING _key",
                "POP": (
                    f"DELETE FROM {tablename} WHERE _key = ? RETURNING _key, {fields}"
                ),
                "CLEAR": f"DELETE FROM {tablename};VACUUM;",
                "POPITEM": (
                    f"DELETE FROM {tablename}"
                    f" WHERE rowid = (SELECT MAX(rowid) FROM {tablename})"
                    f" RETURNING _key, {fields}"
                ),
            }
        )
        self._pragmas = DEFAULT_PRAGMAS.copy()
        self._pragmas.update(self._config.pragmas)
        self._dump_value = self._config.dump_value

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
            con = Connection(self._filename)
            con.set_busy_timeout(int(self._timeout * 1000))

            # Some SQLite pragmas work on a per-connection basis so
            # apply them all on fresh connection
            for key, value in self._pragmas.items():
                con.pragma(key, value)
            self._migrate_table(con, self._config)
            self._local.con = con

        return con

    def close(self) -> None:
        """Close the database connection if open."""
        # The per-thread transaction marker belongs to the connection, so
        # drop it whenever the connection goes away (explicit close or the
        # fork reconnect in ``_con``).
        self._local.in_transaction = False
        super().close()

    @staticmethod
    def _get_field_create(field_tuple):
        field_name, field_type, field_default = field_tuple
        default = " NOT NULL"
        if field_default is None:
            default = " DEFAULT NULL"
        elif field_default is not NO_DEFAULT:
            default = " NOT NULL DEFAULT " + apsw.format_sql_value(field_default)
        name = escape_name(field_name)
        type_ = get_sqlite_type(field_type)
        field_create = f"{name} {type_}{default}"

        return field_create

    @staticmethod
    def _migrate_table(con: Connection, config: ConfigProtocol) -> None:
        """Ensure the table exists, optionally migrating to match config.fields.

        If the table does not exist it is created from *config*.  If
        *config.auto_migrate* is true and the table already exists,
        columns in *config.fields* missing from the table are added.

        Migration uses ``BEGIN IMMEDIATE`` to serialise concurrent
        callers.
        """
        tablename = escape_name(config.tablename)
        existing = {
            row[1] for row in (con.pragma("table_info", config.tablename) or [])
        }

        if not existing:
            primary_key_type = get_sqlite_type(config.key_type)
            fields_create = ", ".join(
                DiskStore._get_field_create(f) for f in config.fields
            )
            create_stmt = (
                f"CREATE TABLE IF NOT EXISTS {tablename} ("
                f" _key {primary_key_type} PRIMARY KEY NOT NULL"
                f", {fields_create})"
            )
            con.execute(create_stmt)
        elif config.auto_migrate:
            config_names = {f[0] for f in config.fields}
            if config_names - existing:
                con.execute("BEGIN IMMEDIATE")
                existing = {
                    row[1] for row in con.pragma("table_info", config.tablename)
                }
                for field_name, field_type, default_value in config.fields:
                    if field_name not in existing:
                        col_def = DiskStore._get_field_create(
                            (field_name, field_type, default_value)
                        )
                        alter_stmt = f"ALTER TABLE {tablename} ADD COLUMN {col_def};"
                        con.execute(alter_stmt)
                con.execute("COMMIT")

    @contextmanager
    def transact(self):
        """Wrapper for performance sensitive bulk writes.

        Every write operation (``__setitem__``, ``__delitem__``, ``add``,
        etc.) runs in an implicit transaction when called outside of
        ``transact()``.  The implicit begin/commit overhead adds up when
        writing many items in a loop:

        .. code:: python

            # slow: one implicit transaction per write
            for i in range(1000):
                store[i] = value

            # fast: single explicit transaction
            with store.transact():
                for i in range(1000):
                    store[i] = value

        Uses ``BEGIN IMMEDIATE`` to avoid deadlocks in concurrent
        workloads.  Nested calls are idempotent (reuse the same
        transaction).  Yields an ``apsw.Cursor`` for callers that need
        direct SQL execution.
        """
        cursor: Cursor = self._con.cursor()
        local = self._local

        # Transaction state is per-thread, matching the per-thread
        # connection.  A thread must never observe another thread's (or a
        # forking process's) transaction, so nested detection uses local
        # state rather than a shared instance attribute.
        if local.in_transaction:
            begin = False
        else:
            cursor.execute("BEGIN IMMEDIATE")
            begin = True
            local.in_transaction = True

        try:
            yield cursor
        except BaseException:
            if begin:
                local.in_transaction = False
                cursor.execute("ROLLBACK")
            cursor.close()
            raise
        else:
            if begin:
                local.in_transaction = False
                cursor.execute("COMMIT")
            cursor.close()

    def __setitem__(self, key: KeyType, value: Any) -> None:
        self._cursor.execute(self._statements["SET"], self._dump_value(key, value))

    def add(self, key: KeyType | None, value: Iterable) -> KeyType | None:
        cursor = self._cursor
        cursor.execute(self._statements["ADD"], self._dump_value(key, value))
        rows = cursor.fetchall()

        if not rows:
            return None

        return rows[0][0]

    def pop(self, key: KeyType, default=MISSING):
        cursor = self._cursor
        cursor.execute(self._statements["POP"], (key,))
        rows = cursor.fetchall()

        if not rows:
            if default is MISSING:
                raise KeyError(key)
            return default

        return self._load_data(rows[0])

    def popitem(self):
        with self.transact() as cursor:
            cursor.execute(self._statements["POPITEM"])
            row = next(cursor, None)
            if not row:
                raise KeyError()
        key = row[0]
        value = self._load_data(row)
        return key, value

    def __delitem__(self, key: KeyType) -> None:
        cursor = self._cursor
        cursor.execute(self._statements["DELETE"], (key,))
        # fetchall() drains the statement so autocheckpoint can run and a
        # later COMMIT is not blocked by an in-progress statement.
        rows = cursor.fetchall()
        if not rows:
            raise KeyError(key)

    def setdefault(self, key: KeyType, default: Iterable | None = None):
        with self.transact():
            try:
                return self[key]
            except KeyError:
                if default is not None:
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

    def update(self, other=(), /, **kwargs):
        """Bulk upsert from a mapping or iterable.

        Wrapped in ``transact()`` so all upserts share a single
        transaction — significantly faster than setting keys
        individually in a loop.
        """
        with self.transact() as cursor:
            if other:
                if isinstance(other, Mapping):
                    cursor.executemany(
                        self._statements["SET"],
                        (self._dump_value(key, value) for key, value in other.items()),
                    )
                elif hasattr(other, "keys"):
                    cursor.executemany(
                        self._statements["SET"],
                        (self._dump_value(key, other[key]) for key in other.keys()),
                    )
                else:
                    cursor.executemany(
                        self._statements["SET"],
                        (self._dump_value(key, value) for key, value in other),
                    )
            if kwargs:
                cursor.executemany(
                    self._statements["SET"],
                    (self._dump_value(key, value) for key, value in kwargs.items()),
                )

    def get_readonly_instance(self):
        return DiskRead(self._filename, self._config)
