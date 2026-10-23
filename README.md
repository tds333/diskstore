# DiskStore Sqlite based disk store

`DiskStore` is a BSD-3-Clause licensed disk storage library, written
in pure Python.

Inspired by DiskCache library it implements a MutableMapping compatible sqlite
based disk storage. Easy interface and very fast. Keys can be of basic sqlite storage
classes defined by Python types `int`, `float`, `str`, `bytes`. Values can be of basic sqlite
type or with custom configuration, supporting dataclasses, structtype, pydantic, ...


## Features

- Pure-Python
- 100% test coverage
- Performance matters
- Thread-safe and process-safe
- Tested on CPython 3.10, 3.11, 3.12, 3.13, 3.14, 3.15
- Optional performance-improved implementation based on APSW.


## Quickstart

Installing `DiskStore`:

  $ uv pip install diskstore

or

  $ uv add diskstore

There are two basic storage classes available:
`DiskStore` and `DiskRead`. Serialisation is controlled by the config classes
in `diskstore.config` (`BaseConfig`, `JsonConfig`, `NamedTupleConfig`,
`DataclassConfig`, `PydanticConfig` and `StructtypeConfig`).

## User Guide

Example:

```python

from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore.db")
ds["key"] = "my value"
print(ds["key"])

```

See the `docs/` directory for the user guide and API reference, or read the
[online documentation](https://tds333.github.io/diskstore/).

### Timings

Rough measurements against `diskcache.Index` on the same machine (Python 3.14.9,
1 KB values, 100 000 operations, one process, median per operation):

| Operation | diskstore (`sqlite3`) | diskstore (`apsw`) | diskcache |
|---|---|---|---|
| get | 3.8 µs | 3.6 µs | 4.3 µs |
| set | 3.1 µs | 2.9 µs | 26.0 µs |
| delete | 18.6 µs | 21.0 µs | 20.3 µs |

Run `make bench` to reproduce. DiskStore is faster because per-operation
overhead is eliminated and the SQL statements are optimised. If
[APSW](https://rogerbinns.github.io/apsw/) is installed
(`pip install diskstore[apsw]`) it is used automatically for further
performance improvements.


License
-------

`DiskStore` is distributed under the terms of the
[BSD-3-Clause](https://spdx.org/licenses/BSD-3-Clause.html) license.

Copyright 2025-2026 Wolfgang Langner
