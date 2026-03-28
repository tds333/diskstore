"""Test diskstore.DiskStore."""

import json
import os.path
import shutil
import tempfile
import uuid
from dataclasses import asdict, dataclass, fields
from decimal import Decimal
from typing import ClassVar, NamedTuple, Optional

import pytest

msgspec = pytest.importorskip("msgspec")
pydantic = pytest.importorskip("pydantic")

import msgspec
import msgspec.json
import pydantic

from diskstore import DiskStore
from diskstore.config import BaseConfig, DataclassConfig, JsonConfig, PydanticConfig


class StructConfig(BaseConfig):
    def __init__(self, struct, tablename=None, key_type=None, timeout=None, **pragmas):
        super().__init__(
            tablename=tablename, key_type=key_type, timeout=timeout, pragmas=pragmas
        )
        self.fields = (("value", "TEXT"),)
        self.struct = struct

    def dump_value(self, value):
        return (msgspec.json.encode(value),)

    def load_data(self, data):
        return msgspec.json.decode(data[0], type=self.struct)


@pytest.fixture
def tmpdir():
    directory = tempfile.mkdtemp(prefix="diskstore-")
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def tmpfilename(tmpdir):
    filename = os.path.join(tmpdir, "diskstore.db")
    return filename


def test_dataclass(tmpfilename) -> None:
    class AddressConfig(DataclassConfig):
        def dump_value(self, value):
            return iter((value.id, value.first_name, value.last_name, str(value.price)))

        def load_data(self, data):
            return self.dataclass(data[0], data[1], data[2], Decimal(data[3]))

    @dataclass
    class Address:
        id: str
        first_name: str
        last_name: str
        price: Decimal = Decimal(0)

    value = Address(
        id=str(uuid.uuid4()), first_name="John", last_name="Doe", price=Decimal(1)
    )
    store = DiskStore(tmpfilename, AddressConfig(dataclass=Address))
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()


def test_dataclass_json(tmpfilename) -> None:
    @dataclass
    class Address:
        id: str
        first_name: str
        last_name: str
        price: Decimal = Decimal(0)

    class AddressConfig(DataclassConfig):
        def __init__(self, tablename=None, key_type=None, timeout=None, **pragmas):
            super().__init__(
                dataclass=Address,
                tablename=tablename,
                key_type=key_type,
                timeout=timeout,
                **pragmas,
            )
            self.fields = (("value", "TEXT"),)

        def dump_value(self, value):
            d = asdict(value)
            d["price"] = str(d["price"])
            return (json.dumps(d),)

        def load_data(self, data):
            value = json.loads(data[0])
            value["price"] = Decimal(value["price"])
            return self.dataclass(**value)

    value = Address(
        id=str(uuid.uuid4()), first_name="John", last_name="Doe", price=Decimal(1)
    )
    store = DiskStore(tmpfilename, AddressConfig())
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()


def test_msgspec_struct(tmpfilename) -> None:
    class Address(msgspec.Struct):
        # _fields: ClassVar
        id: str
        first_name: str
        last_name: str
        street: str
        plz: int
        city: str

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = DiskStore(tmpfilename, StructConfig(Address))
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()


def test_pydantic_model(tmpfilename) -> None:
    class Address(pydantic.BaseModel):
        id: str
        first_name: str
        last_name: str
        street: str
        plz: int
        city: str

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = DiskStore(tmpfilename, PydanticConfig(Address))
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()
