"""Test diskstore.DiskStore."""

import os.path
import shutil
import tempfile
from collections import OrderedDict
from dataclasses import dataclass, fields
from typing import ClassVar

import pytest

from diskstore import DiskRead, DiskStore, Value

data = OrderedDict((key, (str(value),)) for key, value in enumerate(range(10)))


@pytest.fixture(scope="session")
def store_file():
    directory = tempfile.mkdtemp(prefix="diskstore-")
    filename = os.path.join(directory, "diskstore.db")
    with DiskStore(filename) as store:
        store.update(data)
    yield filename
    store.close()
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture(scope="session")
def ro_store(store_file):
    disk_read = DiskRead(store_file)
    yield disk_read
    disk_read.close()


def test_init(store_file) -> None:
    dr = DiskRead(store_file)
    assert dr[0] == Value("0")


def test_init_tablename(store_file) -> None:
    dr = DiskRead(store_file, tablename="Value")
    assert dr[0] == Value("0")


def test_init_value_class(store_file) -> None:
    dr = DiskRead(store_file, value_class=Value)
    assert dr[0] == ("0",)


def test_init_timeout(store_file) -> None:
    dr = DiskRead(store_file, timeout=1.0)
    assert dr[0] == Value("0")


def test_init_error() -> None:
    @dataclass
    class Invalid:
        _fields: ClassVar
        _key: str

    Invalid._fields = tuple(field.name for field in fields(Invalid))

    with pytest.raises(ValueError):
        DiskRead("abc.db", value_class=Invalid)


def test_getitem(ro_store) -> None:
    assert len(ro_store) == len(data)

    for key in data:
        assert ro_store[key] == data[key]


def test_getitem_keyerror(ro_store) -> None:
    with pytest.raises(KeyError):
        ro_store[1000]


def test_get(ro_store) -> None:
    assert ro_store.get(0) == Value("0")
    assert ro_store.get(100, "dne") == "dne"
    assert not ro_store.get(1000)


def test_query_all(ro_store) -> None:
    for key, value in ro_store.query():
        assert data[key] == value


def test_query_one_key(ro_store) -> None:
    result = list(ro_store.query(where="_key=?", parameters=(1,)))
    assert result == [(1, Value("1"))]


def test_query_one_value(ro_store) -> None:
    result = list(ro_store.query(where="value=?", parameters=("1",)))
    assert result == [(1, Value("1"))]


def test_contains(ro_store) -> None:
    assert 9 in ro_store


def test_get_timeout(ro_store) -> None:
    assert ro_store.timeout > 0.0


def test_get_tablename(ro_store) -> None:
    assert ro_store.tablename == "Value"


def test_iter(ro_store) -> None:
    iterator = iter(ro_store)

    assert all(str(key) == ro_store[key][0] for key in iterator)

    with pytest.raises(StopIteration):
        next(iterator)


def test_reversed(ro_store) -> None:
    iterator = reversed(ro_store)

    assert all(str(key) == ro_store[key][0] for key in iterator)

    with pytest.raises(StopIteration):
        next(iterator)


def test_keys(ro_store) -> None:
    keyview = ro_store.keys()
    keys = list(ro_store.keys())
    data_keys = list(data.keys())
    assert len(keyview) == len(data_keys)
    assert keys == data_keys


def test_reversed_keys(ro_store) -> None:
    keyview = ro_store.keys()
    keys = list(reversed(ro_store.keys()))
    data_keys = list(reversed(data.keys()))
    assert len(keyview) == len(data_keys)
    assert keys == data_keys


def test_items(ro_store) -> None:
    items = dict(ro_store.items())
    assert items == data


def test_reversed_items(ro_store) -> None:
    items = dict(reversed(ro_store.items()))
    assert items == data


def test_values(ro_store) -> None:
    values = list(ro_store.values())
    assert values == list(data.values())


def test_reversed_values(ro_store) -> None:
    values = list(reversed(ro_store.values()))
    assert values == list(reversed(data.values()))
