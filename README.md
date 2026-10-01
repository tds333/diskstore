# DiskStore Sqlite based disk store

`DiskStore` is a BSD-3-Clause licensed disk storage library, written
in pure Python.

Inspired by DiskCache library it implements a MutableMapping compatible sqlite
based disk storage. Easy interface and very fast. Keys can be of basic sqlite storage
classes defined by Python types `int`, `float`, `str`, `bytes`. Values can be of basic sqlite
type or with custom configuration, supproting structtype, pydantic, msgspec, ...


## Features

- Pure-Python
- nearly 100% test coverage
- Performance matters
- Thread-safe and process-safe
- Developed on Python 3.15
- Tested on CPython 3.10, 3.11, 3.12, 3.13, 3.14, 3.15
- Optional performance improved implementation bases on apsw.


## Quickstart

Installing `DiskStore`:

  $ uv pip install diskstore

or

  $ uv add diskstore

There are three basic storage classes available.
`DiskStore`, `DiskRead` and `DiskCache`.

## User Guide

Work in progress, look at unit tests for examples.

Example:

```python

from diskstore import DiskStore

ds = DiskStore("/tmp/diskstore.db")
ds["key"] = "my value"
print(ds["key"])

```

See the `docs/` directory for the user guide and API reference.

### Timings

These are rough measurements. Compared to Diskcache library.

Diskstore library:

set: 28.8 µs
get: 3.23 µs
set/delete: 84 µs

Diskcache library:

set: 196 µs
get: 15.5 µs
set/delete: 535 µs


Why is the Diskstore library faster than others. Overhead is eliminated and 
operations where optimized. If [APSW](https://rogerbinns.github.io/apsw/)
is installed (`pip install diskstore[apsw]`) it is used automatically
for furhter performance improvements.


License
-------

`DiskStore` is distributed under the terms of the
[BSD-3-Clause](https://spdx.org/licenses/BSD-3-Clause.html) license.

Copyright 2025-2026 Wolfgang Langner
