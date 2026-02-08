"""Test diskstore.DiskCache."""

import datetime
import io
import os.path
import pathlib
import pickle
import shutil
import tempfile
import time
import warnings
from typing import NamedTuple

import pytest

import diskstore as ds


@pytest.fixture
def tmpdir():
    directory = tempfile.mkdtemp(prefix="diskcache-")
    yield directory
    shutil.rmtree(directory, ignore_errors=True)


@pytest.fixture
def tmpfilename(tmpdir):
    filename = os.path.join(tmpdir, "diskcache.db")
    return filename


@pytest.fixture
def cache(tmpfilename):
    with ds.DiskCache(tmpfilename) as cache:
        yield cache
    cache.close()


def test_init(tmpfilename) -> None:
    cache = ds.DiskCache(tmpfilename)
    cache.check()
    cache.close()


def test_init_filename(tmpfilename):
    path = pathlib.Path(tmpfilename)
    cache = ds.DiskCache(path)
    assert os.path.exists(cache.filename)


def test_binary(tmpfilename) -> None:
    with ds.DiskCache(tmpfilename) as cache:
        values = [b"string"]

        for key, value in enumerate(values):
            cache.set(key, value)

        for key, value in enumerate(values):
            assert cache.get(key) == value


def test_close(tmpfilename) -> None:
    cache = ds.DiskCache(tmpfilename)
    cache.close()


def test_getsetdel(cache) -> None:
    values = [
        1234,
        2**12,
        56.78,
        "hello",
        "hello" * 2**20,
        b"world",
        b"world" * 2**20,
    ]

    for key, value in enumerate(values):
        cache.set(key, value)

    assert len(cache) == len(values)

    for key, value in enumerate(values):
        assert cache.get(key) == value

    for key, _ in enumerate(values):
        cache.delete(key)

    assert len(cache) == 0

    cache.check()


# def test_update_with_itearble(store):
#     values = [
#         (1234, Value("numbers")),
#         (200, Value(False)),
#         (56.78, Value(False)),
#         ("hello", Value(False)),
#         (b"world", Value(False)),
#     ]

#     store.update(values)

#     for key, value in values:
#         assert store[key] == value


# def test_update_with_dict(store) -> None:
#     values = {
#         1234: Value("numbers"),
#         200: Value(False),
#         56.78: Value(False),
#         "hello": Value(False),
#         b"world": Value(False),
#     }

#     store.update(values)

#     for key, value in values.items():
#         assert store[key] == value


# def test_update_with_kv(store):
#     values = {"hello": Value("hello"), "world": Value(False), "some-key": Value("some")}

#     store.update(**values)

#     for key, value in values.items():
#         assert store[key] == value


# def test_update_with_kv_direct(store):
#     store.update(hello=Value("World"), world=Value(False), some_thing=Value(True))

#     assert store["hello"] == Value("World")
#     assert store["world"] == Value(False)
#     assert store["some_thing"] == Value(True)


def test_set_twice(cache) -> None:
    large_value = b"abcd" * 2**5

    cache.set(0, 0)
    cache.set(0, 1)

    assert cache.get(0) == 1

    cache.set(0, large_value)

    assert cache.get(0) == large_value

    cache.set(0, 2)

    assert cache.get(0) == 2

    cache.check()


# def test_setdefault(store) -> None:
#     store[0] = Value("zero")
#     value = store.setdefault(0, Value(0))
#     assert value == Value("zero")

#     value = store.setdefault(1, Value("one"))
#     assert value == Value("one")

#     value = store.setdefault(2, Value(0))
#     assert value == Value(0)


def test_store_bytes(cache) -> None:
    cache.set(0, b"abcd")
    assert cache.get(0) == b"abcd"


def test_custom_value_class_pickle(tmpfilename) -> None:
    class MyPickle(NamedTuple):
        data: bytes
        timestamp: float = 0.0

        @classmethod
        def create(cls, data, timestamp=None):
            data = pickle.dumps(data)
            timestamp = time.time() if timestamp is None else timestamp
            return cls(data, timestamp)

        def decode(self):
            return pickle.loads(self[0])

        @property
        def creation_time(self):
            return datetime.datetime.fromtimestamp(self.timestamp)

        def update_time(self):
            return self._replace(timestamp=time.time())

    data = {"a": 1, "b": 2}
    value = MyPickle.create(data)
    creation_time = value.creation_time
    store = ds.DiskStore(tmpfilename, value_class=MyPickle)
    store["one"] = value
    assert store["one"] == value
    assert type(store["one"].data) == bytes
    assert store["one"].decode() == data
    assert store["one"].creation_time == creation_time
    old_time = value.timestamp
    assert value.update_time() != value
    store.close()


def test_get(cache) -> None:
    assert cache.get(0) is None
    assert cache.get(1, "dne") == "dne"
    assert cache.get(2, {}) == {}


def test_delete(cache) -> None:
    cache.set(0, 0)
    assert cache.delete(0)
    assert len(cache) == 0
    assert not cache.delete(0)
    assert len(cache.check()) == 0


def test_check(cache) -> None:
    blob = b"a" * 2**20
    keys = (0, 1, 1234, 56.78, "hello", b"world")

    for key in keys:
        cache.set(key, blob)

    # Cause mayhem.

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        cache.check()
        cache.check(vacuum=True)

    assert len(cache.check()) == 0  # Should display no warnings.


def test_integrity_check(cache) -> None:
    for value in range(1000):
        cache.set(value, value)

    cache.close()

    with io.open(cache.filename, "r+b") as writer:
        writer.seek(52)
        writer.write(b"\x00\x01")  # Should be 0, change it.

    cache = ds.DiskCache(cache.filename)

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        cache.check()
        cache.check(vacuum=True)

    assert len(cache.check()) == 0
    cache.close()


def test_clear(cache) -> None:
    num_items = 100
    for value in range(num_items):
        cache.set(value, value)
    assert len(cache) == num_items
    cache.clear()
    assert len(cache) == 0
    assert len(cache.check()) == 0


def test_add(cache) -> None:
    assert cache.add(1, 1)
    assert cache.get(1) == 1
    assert not cache.add(1, 2)
    assert cache.get(1) == 1
    cache.delete(1)
    cache.check()


# def test_add_large_value(cache) -> None:
#     value = b"abcd" * 2**20
#     assert cache.add(b"test-key", value)
#     assert cache.get(b"test-key") == value
#     assert not cache.add(b"test-key", value * 2)
#     assert cache.get(b"test-key") == value
#     cache.check()


def test_key_roundtrip(cache):
    key_part_0 = "part0"
    key_part_1 = "part1"
    to_test = [
        key_part_0,
        key_part_1,
    ]

    for key in to_test:
        cache.clear()
        cache.set(key, b"value")
        keys = list(cache)
        assert len(keys) == 1
        store_key = keys[0]
        assert cache.get(key) == b"value"
        # assert store[store_key] == {"example0": ["value0"]}


def test_iter(cache) -> None:
    sequence = list("abcdef")

    for index, value in enumerate(sequence):
        cache.set(value, index)

    iterator = iter(cache)

    assert all(one == two for one, two in zip(sequence, iterator))

    with pytest.raises(StopIteration):
        next(iterator)


def test_iter_error(cache) -> None:
    with pytest.raises(StopIteration):
        next(iter(cache))


def test_reversed(cache) -> None:
    sequence = list("abcdef")

    for index, value in enumerate(sequence):
        cache.set(value, index)

    iterator = reversed(cache)

    pairs = zip(reversed(sequence), iterator)
    assert all(one == two for one, two in pairs)

    with pytest.raises(StopIteration):
        next(iterator)


def test_reversed_error(cache) -> None:
    with pytest.raises(StopIteration):
        next(reversed(cache))


def test_touch(cache) -> None:
    cache.set(0, b"abcd", expire=0.01)
    assert cache.get(0) == b"abcd"
    time.sleep(0.01)
    assert cache.get(0) is None
    assert not cache.get(0) == b"abcd"
    cache.touch(0)
    assert cache.get(0) == b"abcd"
    cache.touch(100)


def test_touch_expire(cache) -> None:
    cache.set(0, b"abcd", expire=0.01)
    assert cache.get(0) == b"abcd"
    time.sleep(0.01)
    assert cache.get(0) is None
    assert not cache.get(0) == b"abcd"
    cache.touch(0, expire=0.1)
    assert cache.get(0) == b"abcd"
    cache.touch(100)


def test_delete_expired(cache) -> None:
    amount = 100
    cache.set("keep", b"abcd", expire=0)
    for key, value in enumerate(range(amount)):
        cache.set(key, value, expire=0.001)
    assert len(cache) == amount + 1
    time.sleep(0.001)
    cache.delete_expired()
    assert len(cache) == 1
