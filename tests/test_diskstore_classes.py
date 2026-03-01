"""Test diskstore.DiskStore."""

import datetime
import io
import json
import os.path
import pathlib
import pickle
import shutil
import tempfile
import threading
import time
import uuid
from collections import namedtuple
from dataclasses import asdict, astuple, dataclass, fields
from decimal import Decimal
from typing import ClassVar, NamedTuple, Optional
from unittest import mock

import pytest

msgspec = pytest.importorskip("msgspec")
pydantic = pytest.importorskip("pydantic")

import msgspec
import pydantic

import diskstore as ds
from diskstore import DiskStore, Value
from diskstore.diskstore import BusyError


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
    @dataclass
    class Address:
        id: str
        first_name: str
        last_name: str
        price: Decimal = Decimal(0)

        def __iter__(self):
            # return iter(astuple(self))
            return iter((self.id, self.first_name, self.last_name, str(self.price)))

        @classmethod
        def _make(cls, data) -> "Address":
            return Address(data[0], data[1], data[2], Decimal(data[3]))

    # Address._fields = fields(Address)

    value = Address(
        id=str(uuid.uuid4()), first_name="John", last_name="Doe", price=Decimal(1)
    )
    store = ds.DiskStore(tmpfilename, value_class=Address)
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

        def __iter__(self):
            return iter(msgspec.structs.astuple(self))

        @classmethod
        def _make(cls, data) -> "Address":
            return Address(*data)

    # Address._fields = Address.__struct_fields__

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = ds.DiskStore(tmpfilename, value_class=Address)
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()


def test_msgspec_struct_single(tmpfilename) -> None:
    class Address(msgspec.Struct):
        _fields: ClassVar = ("data",)
        id: str
        first_name: str
        last_name: str
        street: str
        plz: int
        city: str

        def __iter__(self):
            yield msgspec.json.encode(self)

        @classmethod
        def _make(cls, data) -> "Address":
            return msgspec.json.decode(data[0], type=cls)

    # Address._fields = Address.__struct_fields__

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = ds.DiskStore(tmpfilename, value_class=Address)
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

        def __iter__(self):
            return iter(getattr(self, key) for key in Address.model_fields.keys())

        @classmethod
        def _make(cls, data) -> "Address":
            return Address(**{k: v for k, v in zip(Address.model_fields.keys(), data)})

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = ds.DiskStore(tmpfilename, value_class=Address)
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()


def test_pydantic_model_single(tmpfilename) -> None:
    class Address(pydantic.BaseModel):
        _fields: ClassVar = ("data",)
        id: str
        first_name: str
        last_name: str
        street: str
        plz: int
        city: str

        def __iter__(self):
            yield self.model_dump_json()

        @classmethod
        def _make(cls, data) -> "Address":
            return cls.model_validate_json(data[0])

    value = Address(
        id=str(uuid.uuid4()),
        first_name="John",
        last_name="Doe",
        street="Mainstreet 1",
        plz=77777,
        city="Bonn",
    )
    store = ds.DiskStore(tmpfilename, value_class=Address)
    store[value.id] = value
    # store[value.id] = astuple(value)
    result = store[value.id]
    assert isinstance(result, Address)
    assert result == value
    del store[value.id]
    store.close()
