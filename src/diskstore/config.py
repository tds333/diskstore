"""DiskStore configuration classes and helpers for config."""

import dataclasses
import json
from abc import abstractmethod
from typing import Any, Iterable, Protocol

from .const import TIMEOUT, AnyLite


def get_sqlite_type(type_) -> str:
    if type_ in {"BLOB", "TEXT", "INTEGER", "REAL"}:
        return type_
    sqlite_type = "BLOB"
    if type_ is str:
        sqlite_type = "TEXT"
    elif type_ is int:
        sqlite_type = "INTEGER"
    elif type_ is float:
        sqlite_type = "REAL"

    return sqlite_type


def escape_name(name: str) -> str:
    tablename = '"' + name.replace('"', '""') + '"'
    return tablename


class ConfigProtocol(Protocol):
    """Configuration Protocol

    Attributes:
        tablename: Table name as string
        key_type: key type as string or basic Python type like str, int, float
        timeout: Timeout used to wait if someone blocks connections or with writes
        pragmas: Dictionary with PRAGMAs to set when connections is initialized
        fields: Iterable used for select, update and create statement with
            field name, type and default. `[("value", str), ("value2", int, 0)]`

    """

    tablename: str
    key_type: str
    timeout: float
    pragmas: dict
    fields: Iterable

    @abstractmethod
    def dump_value(self, value: Any) -> Iterable:
        """Called with the value, should return an Iterable
        which is used to write to the DB."""

    @abstractmethod
    def load_data(self, data: tuple) -> Any:
        """load(db_data): called with the tuple selected from DB.
        Should be converted to the type normally received as value."""


class BaseConfig(ConfigProtocol):
    """Base default configuratin."""

    def __init__(self, *, tablename=None, key_type=None, timeout=None, pragmas=None):
        self.tablename = "DiskStore" if tablename is None else tablename
        self.key_type = "BLOB" if key_type is None else get_sqlite_type(key_type)
        self.timeout = TIMEOUT if timeout is None else float(timeout)
        self.pragmas = {} if pragmas is None else pragmas
        self.fields = [("value", "BLOB")]

    def dump_value(self, value: Any) -> Iterable:
        return (value,)

    def load_data(self, data: tuple) -> Any:
        return data[0]


class NamedTupleConfig(BaseConfig):
    def __init__(
        self, value_class, tablename=None, key_type=None, timeout=None, pragmas=None
    ):
        tablename = value_class.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.value_class = value_class
        self.fields = self.get_fields(value_class)

    @staticmethod
    def get_fields(value_class):
        fields = []
        value_columns = tuple(value_class._fields)
        type_annotations = value_class.__annotations__
        value_column_defaults: dict[str, AnyLite] = getattr(
            value_class, "_field_defaults", {}
        )
        for name in value_columns:
            type_cls = type_annotations.get(name, bytes)  # types are optional
            sqlite_type = get_sqlite_type(type_cls)
            default = value_column_defaults.get(name)
            fields.append((name, sqlite_type, default))
        if "_key" in value_columns:
            raise ValueError(
                f"Name _key is not allowed as attribute for {value_class},"
                " listed as field name in _fields."
            )

        return tuple(fields)

    def dump_value(self, value) -> Iterable:
        return value

    def load_data(self, data: tuple) -> Any:
        return self.value_class._make(data)


class JsonConfig(BaseConfig):
    def __init__(self, tablename=None, key_type=None, timeout=None, pragmas=None):
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.fields = (("value", "TEXT"),)

    def dump_value(self, value: Any) -> Iterable:
        return (json.dumps(value),)

    def load_data(self, data: tuple) -> Any:
        return json.loads(data[0])


class DataclassConfig(BaseConfig):
    def __init__(
        self, dataclass, tablename=None, key_type=None, timeout=None, pragmas=None
    ):
        tablename = dataclass.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.dataclass = dataclass
        self.fields = self.get_fields(dataclass)
        self.types = self.get_field_types(dataclass)

    @staticmethod
    def get_fields(dataclass):
        if dataclasses.is_dataclass(dataclass):
            value_columns = tuple(field.name for field in dataclasses.fields(dataclass))
        else:
            raise ValueError("It is not a dataclass.")
        if "_key" in value_columns:
            raise ValueError("Name _key is not allowed as attribute for dataclass.")

        return value_columns

    # fix this, should be in get_fields handled
    @classmethod
    def get_field_types(cls, value_class):
        value_columns = cls.get_fields(value_class)
        if hasattr(value_class, "__annotations__") and value_class.__annotations__:
            type_annotations = value_class.__annotations__
            value_types = []
            for c_name in value_columns:
                type_cls = type_annotations.get(c_name, bytes)  # types are optional
                sqlite_type = get_sqlite_type(type_cls)
                value_types.append(sqlite_type)
        else:
            value_types = ["BLOB" for _ in value_columns]

        return tuple(value_types)

    def dump_value(self, value) -> tuple:
        return dataclasses.astuple(value)

    def load_data(self, data: tuple) -> Any:
        return self.dataclass(*data)


class PydanticConfig(BaseConfig):
    def __init__(
        self, model, tablename=None, key_type=None, timeout=None, pragmas=None
    ):
        tablename = model.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.fields = (("value", "TEXT"),)
        self.model = model

    def dump_value(self, value):
        return (value.model_dump_json(),)

    def load_data(self, data):
        return self.model.model_validate_json(data[0])
