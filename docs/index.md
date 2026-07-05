# DiskStore — SQLite-backed `MutableMapping` / `Mapping` storage

Fast disk storage built on top of the [APSW](https://rogerbinns.github.io/apsw/) SQLite
wrapper.  Keys and values are serialised via a pluggable configuration system
supporting plain blobs, JSON, `NamedTuple`s, dataclasses and Pydantic models.

## Features

- Pure-Python (APSW bindings are C, but the library code is pure Python)
- Nearly 100 % test coverage
- Thread-safe and process-safe (fork‑safe)
- Developed on Python 3.14, tested on CPython 3.10–3.14
- Tested using GitHub Actions on Linux and macOS

## Quickstart

```python
from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore_quickstart.db")
ds["key"] = "my value"
print(ds["key"])
assert len(ds) == 1
assert "key" in ds
assert list(ds) == ["key"]
del ds["key"]
```

## Basic operations

`DiskStore` implements the full `MutableMapping` interface:

```python
from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore_basic.db")

# set
ds["one"] = 1
ds["two"] = 2

# get
assert ds["one"] == 1

# contains
assert "two" in ds

# iteration
assert set(ds) == {"one", "two"}

# length
assert len(ds) == 2

# keys / values / items
assert sorted(ds.keys()) == ["one", "two"]
assert sorted(ds.values()) == [1, 2]

# delete
del ds["one"]
assert len(ds) == 1
```

## Bulk writes

Each individual write (`__setitem__`, `__delitem__`, …) creates an implicit
SQLite transaction.  Wrapping many writes in `transact()` eliminates that
overhead:

```python
from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore_bulk.db")

# slow: one implicit transaction per write
for i in range(100):
    ds[i] = "value"

# fast: single explicit transaction
with ds.transact():
    for i in range(100):
        ds[i] = "value"

# update() is transactional by default
ds.update({i: "value" for i in range(100)})
```

`transact()` yields an `apsw.Cursor` for callers that need direct SQL.
Nested calls are idempotent (the outer transaction is reused).

## Auto-increment keys

When `key_type=int` is used, passing `None` as the key triggers SQLite
auto-increment via `INTEGER PRIMARY KEY NULL → rowid`:

```python
from diskstore import DiskStore
from diskstore.config import BaseConfig

ds = DiskStore("/tmp/diskstore_autoinc.db", config=BaseConfig(key_type=int))

key = ds.add(None, "auto-generated")
assert key is not None
assert isinstance(key, int)
assert ds[key] == "auto-generated"
```

## Other `DiskStore` methods

```python
from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore_misc.db")
ds["one"] = 1
ds["two"] = 2

# pop
assert ds.pop("one") == 1
assert ds.pop("missing", "default") == "default"

# popitem (last inserted item)
_key, _value = ds.popitem()

# setdefault
ds.setdefault("three", 3)
assert ds["three"] == 3

# clear
ds.clear()
assert len(ds) == 0

# check integrity (optional VACUUM)
ds["three"] = 3
warnings = ds.check()
assert warnings == []
ds.check(vacuum=True)

# get read-only instance (shares the same DB file)
ro = ds.get_readonly_instance()
assert "three" in ro
```

## `DiskRead` — read-only access

`DiskRead` is a lightweight `Mapping` implementation that opens the database
read‑only:

```python
from diskstore import DiskRead

# context-manager support (opens and closes the connection)
with DiskRead("/tmp/diskstore_autoinc.db") as ds:
    assert len(ds) > 0
    for key, value in ds.items():
        assert isinstance(key, int)
```

### Query

`DiskRead.query()` supports `WHERE`, `ORDER BY`, `LIMIT` and `OFFSET`:

```python
from diskstore import DiskRead

with DiskRead("/tmp/diskstore_bulk.db") as ds:
    results = list(ds.query(where="_key > ?", parameters=(50,), limit=5))
    assert len(results) == 5
    assert results[0][0] == 51
```

Reversed iteration:

```python
from diskstore import DiskRead

with DiskRead("/tmp/diskstore_bulk.db") as ds:
    keys = list(reversed(ds))
    assert keys == sorted(keys, reverse=True)
```

## Configuration

The config system controls how keys and values are stored in SQLite columns.

### `BaseConfig` — single BLOB column

The default — stores values as raw SQLite BLOBs:

```python
from diskstore import DiskStore
from diskstore.config import BaseConfig

config = BaseConfig(key_type=str)
ds = DiskStore("/tmp/diskstore_config_base.db", config=config)
ds["msg"] = "hello"
assert ds["msg"] == "hello"
```

### `JsonConfig` — JSON-serialised TEXT column

```python
from diskstore import DiskStore
from diskstore.config import JsonConfig

config = JsonConfig(key_type=str)
ds = DiskStore("/tmp/diskstore_config_json.db", config=config)
ds["nested"] = {"a": [1, 2, 3]}
assert ds["nested"] == {"a": [1, 2, 3]}
```

### `NamedTupleConfig` — one column per field

```python
from typing import NamedTuple
from diskstore import DiskStore
from diskstore.config import NamedTupleConfig

class Point(NamedTuple):
    x: float
    y: float

config = NamedTupleConfig(Point, key_type=str)
ds = DiskStore("/tmp/diskstore_config_nt.db", config=config)
ds["origin"] = Point(0.0, 0.0)
pt = ds["origin"]
assert pt.x == 0.0 and pt.y == 0.0
```

### `DataclassConfig` — one column per field

```python
from dataclasses import dataclass
from diskstore import DiskStore
from diskstore.config import DataclassConfig

@dataclass
class Item:
    name: str
    price: float

config = DataclassConfig(Item, key_type=str)
ds = DiskStore("/tmp/diskstore_config_dc.db", config=config)
ds["widget"] = Item("Widget", 9.99)
item = ds["widget"]
assert item.name == "Widget" and item.price == 9.99
```

### `PydanticConfig` — JSON-serialised model column

```python
from pydantic import BaseModel
from diskstore import DiskStore
from diskstore.config import PydanticConfig

class Task(BaseModel):
    title: str
    done: bool = False

config = PydanticConfig(Task, key_type=str)
ds = DiskStore("/tmp/diskstore_config_pd.db", config=config)
ds["task1"] = Task(title="Write docs")
task = ds["task1"]
assert task.title == "Write docs"
assert task.done is False
```

### Configuration options

Every config class accepts:

| Option | Default | Description |
|---|---|---|
| `tablename` | `"DiskStore"` | SQLite table name |
| `key_type` | `bytes` (BLOB) | `int`, `str`, `float`, `bytes` or a SQLite type string |
| `timeout` | `10.0` | Busy timeout in seconds |
| `pragmas` | `{}` | Extra PRAGMAs merged with built-in defaults |
| `auto_migrate` | `True` | Auto-add missing columns at connection start |

### Table migration

When `auto_migrate=True` (the default), `migrate_table()` is called on every
first connection per-process.  It creates the table if absent, then adds any
columns that exist in the config but are missing from the existing table.
Nullable columns (no default) use `NO_DEFAULT`:

```python
from diskstore import DiskStore
from diskstore.config import BaseConfig, NO_DEFAULT

config = BaseConfig(
    tablename="items",
    key_type=int,
)
# Override fields for a multi-column schema:
config.fields = [
    ("name", "TEXT", "unnamed"),      # NOT NULL DEFAULT 'unnamed'
    ("description", "TEXT", None),    # DEFAULT NULL
    ("rating", "INTEGER", NO_DEFAULT),# NOT NULL — must be provided
]
ds = DiskStore("/tmp/diskstore_migrate.db", config=config)
# existing table with fewer columns is automatically extended
```

## Performance notes

DiskStore uses [APSW](https://rogerbinns.github.io/apsw/) instead of the
stdlib `sqlite3` module.  APSW exposes the full SQLite C API, avoids
wrapper overhead, and bundles recent SQLite versions with optimisations
that make single-key operations substantially faster.

**Default pragmas** (`src/diskstore/const.py`):

| Pragma | Value | Effect |
|---|---|---|
| `journal_mode` | `WAL` | Concurrent reads during writes |
| `mmap_size` | 256 MB | Memory-mapped I/O |
| `synchronous` | `NORMAL` | Balance speed / durability |
| `cache_size` | 8,192 pages | 8 MB page cache |

Benchmark scripts are available at `scripts/benchmark_core.py`.

## License

Copyright 2025–2026 Wolfgang Langner

Licensed under the Apache License, Version 2.0 or the MIT license; you may
not use this file except in compliance with one of these licences.
