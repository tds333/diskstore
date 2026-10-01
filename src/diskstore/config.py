"""DiskStore configuration classes and helpers for config."""

import dataclasses
import json
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, Iterable, Protocol

from .const import NO_DEFAULT, TIMEOUT, AnyLite, KeyType


def get_sqlite_type(type_) -> str:
    """Map a Python type or SQLite type name to a SQLite column type.

    ``str`` becomes TEXT, ``int`` and ``bool`` become INTEGER, ``float``
    becomes REAL, and anything else becomes BLOB.  The four names ``"BLOB"``,
    ``"TEXT"``, ``"INTEGER"`` and ``"REAL"`` are passed through unchanged.
    """
    if type_ in {"BLOB", "TEXT", "INTEGER", "REAL"}:
        return type_
    sqlite_type = "BLOB"
    if type_ is str:
        sqlite_type = "TEXT"
    elif type_ is int:
        sqlite_type = "INTEGER"
    elif type_ is bool:
        sqlite_type = "INTEGER"
    elif type_ is float:
        sqlite_type = "REAL"

    return sqlite_type


def escape_name(name: str) -> str:
    """Quote *name* as a SQL identifier, doubling embedded quotes."""
    tablename = '"' + name.replace('"', '""') + '"'
    return tablename


def is_bindable_default(value) -> bool:
    """Whether *value* can be encoded as a SQL literal."""
    return value is None or isinstance(value, (str, bytes, int, float))


class ConfigProtocol(Protocol):
    """Configuration Protocol

    Attributes:
        tablename: Table name as string
        key_type: key type as string or basic Python type like str, int, float
        timeout: Timeout used to wait if someone blocks connections or with writes
        pragmas: Dictionary with PRAGMAs to set when connections is initialized
        fields: Iterable used for select, update and create statement with
            field name, type, default.
            ``[("value", str, NO_DEFAULT), ("value2", int, 0)]``

    """

    tablename: str
    key_type: str
    timeout: float
    pragmas: dict
    fields: Iterable
    auto_migrate: bool

    @abstractmethod
    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """Called with the key and value, should return a Sequence
        with the key as first element, suitable as parameter tuple for
        the INSERT/SET statement."""

    @abstractmethod
    def load_data(self, data: tuple) -> Any:
        """load(db_data): called with the tuple selected from DB,
        including the key at index 0.  Should be converted to the
        type normally received as value."""


class BaseConfig(ConfigProtocol):
    """Default configuration: a single BLOB column named ``value``.

    Values are stored as-is with no serialisation, so anything SQLite can bind
    round-trips unchanged.  This is the config used when none is passed to
    ``DiskStore``.

    Args:
        tablename: SQLite table name, default ``"DiskStore"``.
        key_type: ``int``, ``str``, ``float``, ``bytes`` or a SQLite type name
            such as ``"TEXT"``; see ``get_sqlite_type()``.  ``int`` enables
            auto-increment keys via ``DiskStore.add()``.
        timeout: seconds to wait for a locked database, default ``TIMEOUT``
            (``10.0``).  A negative value is treated as "use the
            default".
        pragmas: extra PRAGMAs merged over ``DEFAULT_PRAGMAS``.
        auto_migrate: create the table if missing and add missing columns at
            connection start, default ``True``.

    Subclass it to change how values are serialised; see ``JsonConfig`` and
    ``DataclassConfig``.
    """

    def __init__(
        self,
        *,
        tablename: str | None = None,
        key_type: Any = None,
        timeout: float | None = None,
        pragmas: dict | None = None,
        auto_migrate: bool | None = None,
    ):
        self.tablename = "DiskStore" if tablename is None else tablename
        self.key_type = "BLOB" if key_type is None else get_sqlite_type(key_type)
        self.timeout = TIMEOUT if timeout is None else float(timeout)
        self.pragmas = {} if pragmas is None else pragmas
        self.auto_migrate = True if auto_migrate is None else auto_migrate
        self.fields = [("value", "BLOB", NO_DEFAULT)]

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """Return *value* as the parameter tuple for INSERT/UPDATE."""
        return (key, value)

    def load_data(self, data: tuple) -> Any:
        """Return the value from a row tuple (key at index 0)."""
        return data[1]


class NamedTupleConfig(BaseConfig):
    """One SQLite column per [typing.NamedTuple][typing.NamedTuple] field.

    Each annotated field becomes a column; type annotations are optional and
    default to ``bytes`` (BLOB) when omitted.  Defaults come from
    ``_field_defaults`` and are only used when they are bindable SQL literals.

    The tablename defaults to the NamedTuple's class name.

    ``_key`` is reserved for the primary key and raises
    [ValueError][ValueError] if used as a field name.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        value_class,
        tablename=None,
        key_type=None,
        timeout=None,
        pragmas=None,
        auto_migrate=None,
    ):
        tablename = value_class.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename,
            key_type=key_type,
            timeout=timeout,
            pragmas=pragmas,
            auto_migrate=auto_migrate,
        )
        self.value_class = value_class
        self.fields = self.get_fields(value_class)

    @staticmethod
    def get_fields(value_class):
        """Build the ``(name, sqlite_type, default)`` column tuples."""
        fields = []
        value_columns = tuple(value_class._fields)
        type_annotations = value_class.__annotations__
        value_column_defaults: dict[str, AnyLite] = getattr(
            value_class, "_field_defaults", {}
        )
        for name in value_columns:
            type_cls = type_annotations.get(name, bytes)  # types are optional
            sqlite_type = get_sqlite_type(type_cls)
            default = value_column_defaults.get(name, NO_DEFAULT)
            if not is_bindable_default(default):
                default = NO_DEFAULT
            fields.append((name, sqlite_type, default))
        if "_key" in value_columns:
            raise ValueError(
                f"Name _key is not allowed as attribute for {value_class},"
                " listed as field name in _fields."
            )

        return tuple(fields)

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """Flatten the NamedTuple into the column parameter tuple."""
        return (key, *value)

    def load_data(self, data: tuple) -> Any:
        """Rebuild the NamedTuple from a row tuple."""
        return self.value_class._make(data[1:])


class JsonConfig(BaseConfig):
    """Store values as JSON in a single TEXT column.

    Values are serialised with [json.dumps][json.dumps] on write and parsed
    with [json.loads][json.loads] on read, so any JSON-serialisable object
    round-trips.  The tablename stays ``"DiskStore"`` — the class name is not
    used.
    """

    def __init__(
        self,
        tablename=None,
        key_type=None,
        timeout=None,
        pragmas=None,
        auto_migrate=None,
    ):
        super().__init__(
            tablename=tablename,
            key_type=key_type,
            timeout=timeout,
            pragmas=pragmas,
            auto_migrate=auto_migrate,
        )
        self.fields = (("value", "TEXT", NO_DEFAULT),)

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """JSON-encode *value* for the single TEXT column."""
        return (key, json.dumps(value))

    def load_data(self, data: tuple) -> Any:
        """JSON-decode the value from a row tuple."""
        return json.loads(data[1])


class DataclassConfig(BaseConfig):
    """One SQLite column per dataclass field.

    Annotations are optional and default to ``bytes`` (BLOB) when omitted.
    A field default becomes the column default only if it is a bindable SQL
    literal (``None``, ``str``, ``bytes``, ``int`` or ``float``); anything
    else is stored as ``NO_DEFAULT``.

    The tablename defaults to the dataclass name.  ``_key`` is reserved for
    the primary key and raises [ValueError][ValueError] if used as a field
    name.

    Because ``_migrate_table()`` adds missing columns, adding a field with a
    default to an existing dataclass migrates old rows without data loss.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        dataclass,
        tablename=None,
        key_type=None,
        timeout=None,
        pragmas=None,
        auto_migrate=None,
    ):
        tablename = dataclass.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename,
            key_type=key_type,
            timeout=timeout,
            pragmas=pragmas,
            auto_migrate=auto_migrate,
        )
        self.dataclass = dataclass
        self.fields = self.get_fields(dataclass)

    @staticmethod
    def get_fields(dataclass):
        """Build the ``(name, sqlite_type, default)`` column tuples."""
        if not dataclasses.is_dataclass(dataclass):
            raise ValueError("It is not a dataclass.")
        dc_fields = dataclasses.fields(dataclass)
        value_columns = tuple(field.name for field in dc_fields)
        if "_key" in value_columns:
            raise ValueError("Name _key is not allowed as attribute for dataclass.")
        type_annotations = getattr(dataclass, "__annotations__", {}) or {}
        fields = []
        for f in dc_fields:
            type_cls = type_annotations.get(f.name, bytes)
            sqlite_type = get_sqlite_type(type_cls)
            if f.default is not dataclasses.MISSING and is_bindable_default(f.default):
                default = f.default
            else:
                default = NO_DEFAULT
            fields.append((f.name, sqlite_type, default))
        return tuple(fields)

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """Flatten the dataclass into the column parameter tuple."""
        return (key, *dataclasses.astuple(value))

    def load_data(self, data: tuple) -> Any:
        """Rebuild the dataclass instance from a row tuple."""
        return self.dataclass(*data[1:])


class PydanticConfig(BaseConfig):
    """Store values as JSON in a single TEXT column, via Pydantic.

    Values are serialised with ``model_dump_json()`` and validated back with
    ``model_validate_json()``, so nested models round-trip.  Requires
    ``pydantic`` (an optional dependency).

    The tablename defaults to the model's class name.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        model,
        tablename=None,
        key_type=None,
        timeout=None,
        pragmas=None,
        auto_migrate=None,
    ):
        tablename = model.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename,
            key_type=key_type,
            timeout=timeout,
            pragmas=pragmas,
            auto_migrate=auto_migrate,
        )
        self.fields = (("value", "TEXT", NO_DEFAULT),)
        self.model = model

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """JSON-encode the model with ``model_dump_json``."""
        return (key, value.model_dump_json())

    def load_data(self, data: tuple) -> Any:
        """Validate the stored JSON back into the model."""
        return self.model.model_validate_json(data[1])


class StructtypeConfig(BaseConfig):
    """Store values as JSON in a single BLOB column, via structtype.

    Values are serialised with ``struct_dump_json()`` and validated back with
    ``struct_validate_json()``.  Requires ``structtype`` (an optional
    dependency); it is faster than Pydantic but validates less strictly.

    The tablename defaults to the struct's class name.
    """

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        struct,
        tablename=None,
        key_type=None,
        timeout=None,
        pragmas=None,
        auto_migrate=None,
    ):
        tablename = struct.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename,
            key_type=key_type,
            timeout=timeout,
            pragmas=pragmas,
            auto_migrate=auto_migrate,
        )
        self.fields = (("value", "BLOB", NO_DEFAULT),)
        self.struct = struct

    def dump_value(self, key: KeyType | None, value: Any) -> Sequence:
        """JSON-encode the struct with ``struct_dump_json``."""
        return (key, value.struct_dump_json())

    def load_data(self, data: tuple) -> Any:
        """Validate the stored JSON back into the struct."""
        return self.struct.struct_validate_json(data[1])
