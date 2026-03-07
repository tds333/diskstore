"""DiskStor configuration classes."""

import dataclasses
import json
from typing import Any, NamedTuple, Protocol

from .const import TIMEOUT, AnyLite


def get_sqlite_type(type_) -> str:
    sqlite_type = "BLOB"
    if type_ is str:
        sqlite_type = "TEXT"
    elif type_ is int:
        sqlite_type = "INTEGER"
    elif type_ is float:
        sqlite_type = "REAL"

    return sqlite_type


class Value(NamedTuple):
    """Basic NamedTuple Value class.

    Usable to create key value mappings.
    """

    value: AnyLite

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


class ConfigProtocol(Protocol):
    tablename: str
    key_type: str
    timeout: float
    pragmas: dict
    fields: tuple
    types: tuple
    defaults: dict

    def dump(self, value: Any) -> tuple: ...

    def load(self, db_data: tuple) -> Any: ...


class BaseConfig:
    def __init__(self, *, tablename=None, key_type=None, timeout=None, pragmas=None):
        self.tablename = "DiskStore" if tablename is None else tablename
        self.key_type = "BLOB" if key_type is None else get_sqlite_type(key_type)
        self.timeout = TIMEOUT if timeout is None else float(timeout)
        self.pragmas = {} if pragmas is None else pragmas
        self.fields = ("value",)
        self.types = ("BLOB",)
        self.defaults = {}

    def dump(self, value: Any) -> tuple:
        return (value,)

    def load(self, db_data: tuple) -> Any:
        return db_data[0]


class NamedTupleConfig(BaseConfig):
    def __init__(
        self, value_class=Value, tablename=None, key_type=None, timeout=None, **pragmas
    ):
        tablename = value_class.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.value_class = value_class
        self.fields = self.get_fields(value_class)
        self.types = self.get_field_types(value_class)
        self.defaults = self.get_field_defaults(value_class)

    @staticmethod
    def get_fields(value_class):
        if hasattr(value_class, "_fields"):
            value_columns = tuple(value_class._fields)
        # elif dataclasses.is_dataclass(value_class):
        #     value_columns = tuple(
        #         field.name for field in dataclasses.fields(value_class)
        #     )
        # elif hasattr(value_class, "__struct_fields__"):  # support msgspec.Struct
        #     value_columns = tuple(value_class.__struct_fields__)
        # elif hasattr(value_class, "model_fields"):  # support pydantic.BaseModel
        #     value_columns = tuple(value_class.model_fields)
        # elif hasattr(value_class, "__annotations__"):
        #     value_columns = tuple(value_class.__annotations__)
        else:
            raise AttributeError(
                f"Class for values {value_class} has no attibute '_fields'."
            )
        if "_key" in value_columns:
            raise ValueError(
                f"Name _key is not allowed as attribute for {value_class},"
                " listed as field name in _fields."
            )

        return value_columns

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

    @staticmethod
    def get_field_defaults(value_class) -> dict[str, AnyLite]:
        value_column_defaults: dict[str, AnyLite] = getattr(
            value_class, "_field_defaults", {}
        )
        return value_column_defaults

    def dump(self, value) -> tuple:
        return value

    def load(self, db_data: tuple) -> Any:
        return self.value_class._make(db_data)


class JsonConfig(BaseConfig):
    def __init__(self, tablename=None, key_type=None, timeout=None, **pragmas):
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.fields = ("value",)
        self.types = ("TEXT",)
        self.defaults = {}

    def dump(self, value: Any) -> tuple:
        return (json.dumps(value),)

    def load(self, db_data: tuple) -> Any:
        return json.loads(db_data[0])


class DataclassConfig(BaseConfig):
    def __init__(
        self, dataclass, tablename=None, key_type=None, timeout=None, **pragmas
    ):
        tablename = dataclass.__name__ if not tablename else tablename
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.dataclass = dataclass
        self.fields = self.get_fields(dataclass)
        self.types = self.get_field_types(dataclass)
        # self.defaults = {}  # ToDo: implement it

    @staticmethod
    def get_fields(dataclass):
        if dataclasses.is_dataclass(dataclass):
            value_columns = tuple(field.name for field in dataclasses.fields(dataclass))
        else:
            raise ValueError(f"It is not a dataclass.")
        if "_key" in value_columns:
            raise ValueError("Name _key is not allowed as attribute for dataclass.")

        return value_columns

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

    # @staticmethod
    # def get_field_defaults(value_class) -> dict[str, AnyLite]:
    #     value_column_defaults: dict[str, AnyLite] = getattr(
    #         value_class, "_field_defaults", {}
    #     )
    #     return value_column_defaults

    def dump(self, value) -> tuple:
        return dataclasses.astuple(value)

    def load(self, db_data: tuple) -> Any:
        return self.dataclass(*db_data)
