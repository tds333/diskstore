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
from typing import ClassVar, NamedTuple, Optional
from unittest import mock

import pytest

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


@pytest.fixture
def store(tmpfilename):
    with ds.DiskStore(tmpfilename, value_class=Value) as store:
        yield store
    store.close()
    # if store.filename and store.filename != ":memory:":
    #     os.remove(store.filename)


def test_init(tmpfilename) -> None:
    store = ds.DiskStore(tmpfilename)
    assert store.check() == []
    store.close()


def test_init_tablename(tmpfilename) -> None:
    store = ds.DiskStore(tmpfilename, tablename="other table 1")
    assert store.check() == []
    store.close()


def test_init_tablename_escape(tmpfilename) -> None:
    store = ds.DiskStore(tmpfilename, tablename='other "table" 1')
    store[0] = Value(0)
    assert store.check() == []
    store.close()


def test_init_filename(tmpfilename):
    path = pathlib.Path(tmpfilename)
    store = ds.DiskStore(path)
    assert os.path.exists(store.filename)


def test_alter(tmpfilename) -> None:
    class MyData(NamedTuple):
        name: str
        timestamp: float = time.time()

    # class MyNewData(MyData):
    #     label: str = ""

    class MyNewData(NamedTuple):
        name: str
        timestamp: float = time.time()
        label: str = ""
        number: int = 0
        offset: float = 1.1
        garbage: bytes = b"A"

    base = DiskStore(tmpfilename, value_class=MyData, tablename="data")
    values = []
    for i in range(10):
        value = MyData(f"my number {i}")
        base[i] = value
        values.append(value)

    assert len(values) == len(base)
    for key, value in enumerate(values):
        assert base[key] == value

    base_new = DiskStore(
        filename=base.filename, value_class=MyNewData, tablename="data"
    )
    assert len(values) == len(base_new)
    for key, value in enumerate(values):
        print(base_new[key])
        new_value = MyNewData(*value)
        assert base_new[key] == new_value
    # old still works (without label)
    for key, value in enumerate(values):
        print(base[key])
        assert base[key] == value

    base.close()


def test_binary(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename) as store:
        values = [
            Value(
                b"string",
            )
        ]

        for key, value in enumerate(values):
            store[key] = value

        for key, value in enumerate(values):
            assert store[key] == value


def test_key_type_int(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename, key_type=int) as store:
        store[0] = Value("0")
        store["1"] = Value("1")
        # special, integer primary key is rowid and NULL values get
        # rowid incremental assigned
        store[None] = Value("2")
        store[None] = Value("3")
        assert store[0] == Value("0")
        assert store[1] == Value("1")
        assert store[2] == Value("2")
        assert store[3] == Value("3")
        store[1] = Value("one")
        assert store["1"] == Value("one")
        assert store[1] == Value("one")
        assert store[1.0] == Value("one")
        assert store["1.0"] == Value("one")


def test_key_type_str(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename, key_type=str) as store:
        store["1"] = Value("1")
        assert store["1"] == Value("1")
        store[1] = Value("2")
        assert store[1] == Value("2")


def test_key_type_float(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename, key_type=float) as store:
        store[0.0] = Value("0")
        store["1.0"] = Value("1")
        assert store[0] == Value("0")
        assert store[1] == Value("1")
        store[1.0] = Value("one")
        assert store["1.0"] == Value("one")


def test_key_type_blob(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename) as store:
        store["1"] = Value("1")
        assert store["1"] == Value("1")
        store[1] = Value("2")
        assert store[1] == Value("2")
        assert store["1"] == Value("1")
        store[1.0] = Value("1.0")
        assert store[1.0] == Value("1.0")


def test_close_error(store) -> None:
    class LocalTest(object):
        def __init__(self):
            self._calls = 0

        def __getattr__(self, name):
            if self._calls:
                raise AttributeError
            self._calls += 1
            return mock.Mock()

    with mock.patch.object(store, "_local", LocalTest()):
        store.close()


def test_getsetdel(store) -> None:
    values = [
        Value(1234),
        Value(2**12),
        Value(56.78),
        Value("hello"),
        Value("hello" * 2**10),
        Value(b"world"),
        Value(b"world" * 2**10),
    ]

    for key, value in enumerate(values):
        store[key] = value

    assert len(store) == len(values)

    for key, value in enumerate(values):
        assert store[key] == value

    for key, _ in enumerate(values):
        del store[key]

    assert len(store) == 0

    assert store.check() == []


def test_getsetdel_memory() -> None:
    store = DiskStore(":memory:")
    values = [
        Value(1234),
        Value(2**12),
        Value(56.78),
        Value("hello"),
        Value("hello" * 2**10),
        Value(b"world"),
        Value(b"world" * 2**10),
    ]

    for key, value in enumerate(values):
        store[key] = value

    assert len(store) == len(values)

    for key, value in enumerate(values):
        assert store[key] == value

    for key, _ in enumerate(values):
        del store[key]

    assert len(store) == 0

    assert store.check() == []


def test_value(store) -> None:
    values = {
        "i": Value(1234),
        "f": Value(56.78),
        "s": Value("hello"),
        "b": Value(b"world"),
    }

    store.update(values)

    assert len(store) == len(values)
    assert int(store["i"]) == 1234
    assert float(store["f"]) == 56.78
    assert bytes(store["b"]) == b"world"
    assert str(store["s"]) == "hello"
    assert repr(store["s"]) == "Value(value='hello')"
    assert str(store["b"]) == "b'world'"

    with pytest.raises(ValueError):
        int(store["f"])
    with pytest.raises(ValueError):
        float(store["i"])
    with pytest.raises(ValueError):
        bytes(store["s"])


def test_get_readonly_instance(store) -> None:
    values = [
        Value(1234),
        Value(2**12),
        Value(56.78),
        Value("hello"),
        Value("hello" * 2**10),
        Value(b"world"),
        Value(b"world" * 2**10),
    ]

    store.update(enumerate(values))
    ro_store = store.get_readonly_instance()

    assert len(ro_store) == len(values)

    for key, value in enumerate(values):
        assert ro_store[key] == value


def test_update_with_itearble(store):
    values = [
        (1234, Value("numbers")),
        (200, Value(False)),
        (56.78, Value(False)),
        ("hello", Value(False)),
        (b"world", Value(False)),
    ]

    store.update(values)

    for key, value in values:
        assert store[key] == value


def test_update_with_dict(store) -> None:
    values = {
        1234: Value("numbers"),
        200: Value(False),
        56.78: Value(False),
        "hello": Value(False),
        b"world": Value(False),
    }

    store.update(values)

    for key, value in values.items():
        assert store[key] == value


def test_update_with_keys(store) -> None:
    class MyData:
        def __init__(self):
            self.values = {
                1234: Value("numbers"),
                200: Value(False),
                56.78: Value(False),
                "hello": Value(False),
                b"world": Value(False),
            }

        def keys(self):
            return self.values.keys()

        def __getitem__(self, key):
            return self.values[key]

    data = MyData()
    store.update(data)

    for key, value in data.values.items():
        assert store[key] == value


def test_update_with_kv(store):
    values = {"hello": Value("hello"), "world": Value(False), "some-key": Value("some")}

    store.update(**values)

    for key, value in values.items():
        assert store[key] == value


def test_update_with_kv_direct(store):
    store.update(hello=Value("World"), world=Value(False), some_thing=Value(True))

    assert store["hello"] == Value("World")
    assert store["world"] == Value(False)
    assert store["some_thing"] == Value(True)


def test_update_from_other_diskstore(tmpdir) -> None:
    class MyData(NamedTuple):
        name: str
        timestamp: float = time.time()

    base = DiskStore(os.path.join(tmpdir, "base.db"), value_class=MyData)
    other = DiskStore(os.path.join(tmpdir, "other.db"), value_class=MyData)
    values = []
    for i in range(10):
        value = MyData(f"my number {i}")
        base[i] = value
        values.append(value)

    other.update(base)

    assert other == base
    assert len(values) == len(other)
    base.close()
    other.close()


def test_update_with_dict_interesting_data(store) -> None:
    values = {
        "hello my world": Value(False),
        "hello\nmy world": Value(False),
        "helloäüö": Value(False),
        "select * from Value;": Value(False),
        "s": Value("select * from Value;"),
        "d": Value("delete from Value;"),
        b"hello world": Value(False),
        b"0": Value(b"\0"),
        b"\0": Value(b"0"),
    }

    store.update(values)

    for key, value in values.items():
        assert store[key] == value


def test_ds_with_key(tmpfilename) -> None:
    class MyData(NamedTuple):
        id: int
        name: str
        timestamp: float = time.time()

    base = DiskStore(tmpfilename, value_class=MyData)
    values = []
    for i in range(10):
        value = MyData(i, f"my number {i}")
        base[i] = value
        values.append(value)

    base.close()


def test_get_keyerror1(store) -> None:
    with pytest.raises(KeyError):
        store[0]


def test_setitem(store) -> None:
    store[0] = Value(0)
    assert store[0] == Value(0)
    store[0] = Value(1)

    assert store[0] == Value(1)
    with pytest.raises(ValueError):
        store[1] = "mystring"


def test_getitem(store) -> None:
    store[0] = Value(0)
    assert store[0] == Value(0)
    assert store[0] == (0,)


def test_set_twice(store) -> None:
    large_value = b"abcd" * 2**10

    store[0] = Value(0)
    store[0] = Value(1)

    assert store[0] == Value(1)

    store[0] = Value(large_value)

    assert store[0] == Value(large_value)

    store[0] = Value(2)

    assert store[0] == Value(2)

    assert not store.check()


def test_setdefault(store) -> None:
    store[0] = Value("zero")
    value = store.setdefault(0, Value(0))
    assert value == Value("zero")

    value = store.setdefault(1, Value("one"))
    assert value == Value("one")

    value = store.setdefault(2, None)
    assert value is None


def test_store_bytes(store) -> None:
    store[0] = Value(b"abcd")
    assert store[0] == Value(b"abcd")


def test_store_other_class(tmpfilename) -> None:
    class MyValue(NamedTuple):
        v1: str
        v2: int
        v3: float
        v4: bytes

    value = MyValue("one", 1, 1.1, b"100")
    store = ds.DiskStore(tmpfilename, value_class=MyValue)
    store["one"] = value
    assert store["one"] == value
    store.close()


def test_store_collections_namedtuple(tmpfilename) -> None:
    MyValue = namedtuple("MyValue", ["v1", "v2"])

    value = MyValue("one", 1)
    store = ds.DiskStore(tmpfilename, value_class=MyValue)
    store["one"] = value
    assert store["one"] == value
    store.close()


def test_custom_json_value_class(tmpfilename) -> None:
    class MyJson(NamedTuple):
        data: str

        @classmethod
        def create(cls, data):
            data = json.dumps(data)
            return cls(data)

        def decode(self):
            return json.loads(self[0])

    data_dict = {"a": 1, "b": 2}
    value = MyJson.create(data_dict)
    store = ds.DiskStore(tmpfilename, value_class=MyJson)
    store["one"] = value
    assert store["one"] == value
    assert store["one"].decode() == data_dict
    store.close()


def test_address_value_class() -> None:
    class Address(NamedTuple):
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
    store = ds.DiskStore("", value_class=Address)
    store[value.id] = value
    assert store[value.id] == value
    store.close()


def test_dataclass(tmpfilename) -> None:
    @dataclass
    class Address:
        _fields: ClassVar
        id: str
        first_name: str
        last_name: str
        street: str
        plz: int
        city: str

        def __iter__(self):
            return iter(astuple(self))

    Address._fields = tuple(field.name for field in fields(Address))

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
    del store[value.id]
    # store[value.id] = astuple(value)
    # assert store[value.id] == value
    store.close()


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


def test_custom_value_class_jsonb(tmpfilename) -> None:
    from apsw import jsonb_decode, jsonb_encode

    @dataclass
    class MyData:
        a: int
        b: int
        timestamp: Optional[float] = None

    class MyJsonB(NamedTuple):
        data: bytes
        timestamp: float = 0.0

        @classmethod
        def create(cls, datac):
            data_dict = asdict(datac)
            timestamp = data_dict.setdefault("timestamp", time.time())
            data = jsonb_encode(data_dict)
            return cls(data, timestamp)

        def convert(self):
            data = jsonb_decode(self[0])
            data["timestamp"] = self.timestamp
            return MyData(**data)

        @property
        def creation_time(self):
            return datetime.datetime.fromtimestamp(self.timestamp)

        def update_time(self):
            return self._replace(timestamp=time.time())

    datac = MyData(1, 2, time.time())
    value = MyJsonB.create(datac)
    creation_time = value.creation_time
    store = ds.DiskStore(tmpfilename, value_class=MyJsonB)
    store["one"] = value
    assert store["one"] == value
    assert type(store["one"].data) == bytes
    assert store["one"].convert() == datac
    assert store["one"].creation_time == creation_time
    mydata = value.convert()
    assert mydata.a == 1
    assert mydata.b == 2
    old_time = value.timestamp
    assert value.update_time() != old_time
    store.close()


def test_get(store) -> None:
    assert store.get(0) is None
    assert store.get(1, "dne") == "dne"
    assert store.get(2, {}) == {}


def test_query_all(store) -> None:
    store["one"] = Value(1)
    result = list(store.query())
    assert result == [("one", Value(1))]


def test_query_one_key(store) -> None:
    store["one"] = Value(1)
    result = list(store.query(where="_key=?", parameters=("one",)))
    assert result == [("one", Value(1))]


def test_query_one_value(store) -> None:
    store["one"] = Value(1)
    result = list(store.query(where="value=?", parameters=(1,)))
    assert result == [("one", Value(1))]


def test_query_value(store) -> None:
    for i in range(1, 10):
        store[f"order#{i}"] = Value(i)
    result = list(store.query(where="value = 5"))
    assert result == [("order#5", Value(5))]


def test_query_rowid(store) -> None:
    for i in range(1, 10):
        store[f"order#{i}"] = Value(i)
    result = list(store.query(where="rowid = 5"))
    assert result == [("order#5", Value(5))]


def test_query_multiple(store) -> None:
    expected = [(b"order#1", Value("my data 1"))]
    for i in range(1, 100):
        key = b"order#%d" % i
        value = f"my data {i}"
        store[key] = Value(value)
        if i >= 10 and i < 20:
            expected.append((key, Value(value)))

    # result = list(store.query(where="_key LIKE 'order#1%'"))
    result = list(
        store.query(where="_key LIKE ?", parameters=("order#1%",), order="_key ASC")
    )
    assert result == expected


def test_query_big(store) -> None:
    store.update(((f"order#{i}", Value(i)) for i in range(1, 10_000)))
    t0 = time.time()
    result = list(store.query(where="value = 5000"))
    t1 = time.time()
    duration = t1 - t0
    print(f"duration: {duration}")
    assert result == [("order#5000", Value(5000))]


def test_query_multiple_big(store) -> None:
    amount = 10_000
    expected = int(amount / 100)
    store.update(((f"order#{i}", Value(i % 100)) for i in range(1, amount)))
    t0 = time.time()
    result = list(store.query(where="value = 50"))
    t1 = time.time()
    duration = t1 - t0
    print(f"duration: {duration}")
    assert len(result) == expected


def test_query_json_value(store) -> None:
    for i in range(1, 100):
        key = f"{i}"
        data = {"data": f"mydata {i}", "key": i}
        value = Value(json.dumps(data))
        store[key] = value

    expected = Value(json.dumps({"data": "mydata 50", "key": 50}))

    result = list(store.query(where="value->>'$.key' = 50"))
    assert result
    assert result[0][1] == expected


def test_query_jsonb_value(store) -> None:
    from apsw import jsonb_encode

    for i in range(1, 100):
        key = f"{i}"
        data = {"data": f"mydata {i}", "key": i}
        value = Value(jsonb_encode(data))
        store[key] = value

    expected = Value(jsonb_encode({"data": "mydata 50", "key": 50}))

    result = list(store.query(where="value->>'$.key' = 50"))
    assert result
    assert result[0][1] == expected


def test_pop(store) -> None:
    store["alpha"] = Value(1)
    assert store.pop("alpha") == Value(1)
    assert store.get("alpha") is None
    assert store.check() == []

    time.sleep(0.01)
    assert store.pop("beta", "dne") == "dne"

    assert store.pop("dne", None) is None

    store["delta"] = Value(210)

    store["epsilon"] = Value("0" * 2**10)
    assert store.pop("epsilon") == Value("0" * 2**10)

    with pytest.raises(KeyError):
        store.pop("miss")


def test_popitem(store) -> None:
    store["alpha"] = Value(1)
    assert store.popitem() == ("alpha", Value(1))
    assert store.get("alpha") is None
    assert store.check() == []
    with pytest.raises(KeyError):
        store.popitem()


def test_del(store) -> None:
    store[1] = Value("one")
    del store[1]
    assert len(store) == 0
    with pytest.raises(KeyError):
        del store[0]
    assert len(store.check()) == 0


def test_check(store) -> None:
    blob = b"a" * 2**10
    keys = (0, 1, 1234, 56.78, "hello", b"world")

    for key in keys:
        store[key] = Value(blob)

    assert len(store.check()) == 0  # Should display no warnings.


def test_check_vacuum(store) -> None:
    blob = b"a" * 2**10
    keys = (0, 1, 1234, 56.78, "hello", b"world")

    for key in keys:
        store[key] = Value(blob)

    assert len(store.check(vacuum=True)) == 0  # Should display no warnings.


def test_integrity_check(store) -> None:
    for value in range(1000):
        store[value] = Value(value)

    store.close()

    with io.open(store.filename, "r+b") as writer:
        writer.seek(52)
        writer.write(b"\x00\x01")  # Should be 0, change it.

    store = ds.DiskStore(store.filename)

    warns = store.check(vacuum=True)

    assert len(warns) > 0
    print(warns)

    assert len(store.check()) == 0
    store.close()


def test_clear(store) -> None:
    num_items = 100
    for value in range(num_items):
        store[value] = Value(value)
    assert len(store) == num_items
    store.clear()
    assert len(store) == 0
    assert len(store.check()) == 0


def test_with(store) -> None:
    with ds.DiskStore(store.filename, value_class=Value) as tmp:
        tmp["a"] = Value(0)
        tmp["b"] = Value(1)

    assert store["a"] == Value(0)
    assert store["b"] == Value(1)


def test_contains(store) -> None:
    assert 0 not in store
    store[0] = Value(0)
    assert 0 in store


def test_add(store) -> None:
    assert store.add(1, Value(1)) == 1
    assert store.get(1) == Value(1)
    assert store.add(1, Value(2)) is None
    assert store.get(1) == Value(1)
    del store[1]
    assert store.check() == []


def test_add_key_type_int(tmpfilename) -> None:
    with ds.DiskStore(tmpfilename, key_type=int) as store:
        assert store.add(None, Value(0)) == 1
        assert store.get(1) == Value(0)
        assert store.add(1, Value(1)) is None
        assert store.add(2, Value(2)) == 2
        assert store.get(2) == Value(2)
        del store[1]
        assert store.check() == []


def test_add_large_value(store) -> None:
    value = Value(b"abcd" * 2**10)
    assert store.add(b"test-key", value)
    assert store.get(b"test-key") == value
    assert not store.add(b"test-key", Value(value.value * 2))
    assert store.get(b"test-key") == value
    assert store.check() == []


def test_iter(store) -> None:
    sequence = list("abcdef")

    for index, value in enumerate(sequence):
        store[value] = Value(index)

    iterator = iter(store)

    assert all(one == two for one, two in zip(sequence, iterator))

    with pytest.raises(StopIteration):
        next(iterator)


def test_iter_error(store) -> None:
    with pytest.raises(StopIteration):
        next(iter(store))


def test_reversed(store) -> None:
    sequence = list("abcdef")

    for index, value in enumerate(sequence):
        store[value] = Value(index)

    iterator = reversed(store)

    pairs = zip(reversed(sequence), iterator)
    assert all(one == two for one, two in pairs)

    with pytest.raises(StopIteration):
        next(iterator)


def test_reversed_error(store) -> None:
    with pytest.raises(StopIteration):
        next(reversed(store))


def test_iteritems_with_query(store) -> None:
    sequence = list("abcdef")
    comp_dict = {}

    for index, value in enumerate(sequence):
        store[value] = Value(index)
        comp_dict[value] = Value(index)

    new_dict = dict(store.query())

    assert new_dict == comp_dict


def test_items(store) -> None:
    sequence = list("abcdef")
    comp_dict = {}

    for index, value in enumerate(sequence):
        store[value] = Value(index)
        comp_dict[value] = Value(index)

    new_dict = dict(store.items())

    assert new_dict == comp_dict


def test_values(store) -> None:
    sequence = list("abcdef")
    comp_values = []

    for index, value in enumerate(sequence):
        store[value] = Value(index)
        comp_values.append(Value(index))

    new_values = list(store.values())

    assert new_values == comp_values


def test_keys(store) -> None:
    sequence = list("abcdef")

    for index, value in enumerate(sequence):
        store[value] = Value(index)

    new_keys = list(store.keys())

    assert new_keys == sequence


def test_pickle(store):
    for num, val in enumerate("abcde"):
        store[val] = Value(num)

    data = pickle.dumps(store)
    other = pickle.loads(data)

    for key in other:
        assert other[key] == store[key]


def test_pragmas(store) -> None:
    results = []

    def compare_pragmas():
        valid = True

        for key, value in ds.DEFAULT_PRAGMAS.items():
            if not key.startswith("sqlite_"):
                continue

            pragma = key[7:]

            result = store._sql("PRAGMA %s" % pragma).fetchall()

            if result == [(value,)]:
                continue

            args = pragma, result, [(value,)]
            print("pragma %s mismatch: %r != %r" % args)
            valid = False

        results.append(valid)

    threads = []

    for _count in range(8):
        thread = threading.Thread(target=compare_pragmas)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    assert all(results)


def test_key_roundtrip(store):
    key_part_0 = "part0"
    key_part_1 = "part1"
    to_test = [
        key_part_0,
        key_part_1,
    ]

    for key in to_test:
        store.clear()
        store[key] = Value(b"value")
        keys = list(store)
        assert len(keys) == 1
        store_key = keys[0]
        assert store[key] == Value(b"value")
        # assert store[store_key] == {"example0": ["value0"]}


def test_copy() -> None:
    store_dir1 = tempfile.mkdtemp()

    with ds.DiskStore(os.path.join(store_dir1, "diskstore.db")) as store1:
        for count in range(10):
            store1[count] = Value(str(count))

        for count in range(10, 20):
            store1[count] = Value(str(count) * int(1e5))

    store_dir2 = tempfile.mkdtemp()
    shutil.rmtree(store_dir2)
    shutil.copytree(store_dir1, store_dir2)

    with ds.DiskStore(os.path.join(store_dir2, "diskstore.db")) as store2:
        for count in range(10):
            assert store2[count] == Value(str(count))

        for count in range(10, 20):
            assert store2[count] == Value(str(count) * int(1e5))

    shutil.rmtree(store_dir1, ignore_errors=True)
    shutil.rmtree(store_dir2, ignore_errors=True)


def test_differnt_threads(store) -> None:
    results = {}

    def thread_run(number):
        assert store.get(number, None) is None
        store[number] = Value(f"{number}")
        del store[number]
        store[number] = Value(f"{number}")
        results[number] = Value(f"{number}")

    threads = []

    for number in range(8):
        thread = threading.Thread(target=thread_run, args=(number,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    for key, value in results.items():
        assert Value(str(key)) == value


def test_transact(store):
    values = [
        Value(1234),
        Value(56.78),
        Value("hello"),
        Value(b"world"),
    ]

    with store.transact() as cx:
        store.update(enumerate(values))
        store[10] = Value(10)
        assert store[10] == Value(10)
        assert store[0] == Value(1234)

    for key, value in enumerate(values):
        assert store[key] == value

    assert store[10] == Value(10)


def test_transact_rollback(store):
    with pytest.raises(ValueError):  # noqa: PT011, PT012
        with store.transact():
            store[10] = Value(10)
            raise ValueError("TEST")

    assert 10 not in store


def test_transact_twice_rollback(store):
    with pytest.raises(ValueError):  # noqa: PT011, PT012
        with store.transact():
            with store.transact():
                store[10] = Value(10)
                raise ValueError("TEST")

    assert 10 not in store


def test_timeout(tmpfilename):
    values = [
        Value(1234),
        Value(56.78),
        Value("hello"),
        Value(b"world"),
    ]

    store = DiskStore(tmpfilename, value_class=Value, timeout=0.1)

    with store.transact() as cx:
        store.update(enumerate(values))
        store[10] = Value(10)

    for key, value in enumerate(values):
        assert store[key] == value

    assert store[10] == Value(10)

    store.close()


def test_busy(tmpfilename):
    store = DiskStore(tmpfilename, value_class=Value, timeout=0.001)

    def thread_run():
        with store.transact(retry=False):
            store[1] = Value("2")
            time.sleep(0.05)

    thread = threading.Thread(target=thread_run)
    thread.start()

    time.sleep(0.01)
    with pytest.raises(BusyError):  # noqa: PT012
        with store.transact(retry=False):
            store[1] = Value("1")

    thread.join()
    store.close()


def test_busy_retry(tmpfilename):
    store = DiskStore(tmpfilename, value_class=Value, timeout=0.01)

    def thread_run():
        with store.transact(retry=False):
            store[1] = Value("2")
            time.sleep(0.03)

    thread = threading.Thread(target=thread_run)
    thread.start()

    time.sleep(0.02)
    with store.transact(retry=True):
        store[1] = Value("1")

    thread.join()
    store.close()
