# DiskStore Sqlite based disk store

`DiskStore`_ is an Apache2/MIT licensed disk storage library, written
in pure Python.

Inspired by DiskCache library it implements a MutableMapping compatible sqlite
based disk storage. Easy interface and very fast. Keys can be of basic sqlite storage
classes defined by Python types `int`, `float`, `str`, `bytes`. Value is a `Namedtuple` class
with same basic Python types.


## Features

- Pure-Python
- nearly 100% test coverage
- Performance matters
- Thread-safe and process-safe
- Developed on Python 3.10
- Tested on CPython 3.10, 3.11, 3.12, 3.13, 3.14, 3.15
- Tested using GitHub Actions


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

from diskstore import DiskStore, Value

ds = DiskStore("/tmp/diskstore.db", value_class=Value)
ds["key"] = Value("my value")
print(ds["key"])

```

Everything is mostly stable and test coverage is nearly 100%. Documentation is missing.

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


Why is the Diskstore library faster than others. Every overhead is eliminitated and it uses the
faster sqlite library apsw with up to date sqlite library version. 


License
-------

Copyright 2025-2026 Wolfgang Langner

Licensed under the Apache License, Version 2.0 (the "License") or MIT; you may not use
this file except in compliance with the License.  You may obtain a copy of the
License at

    http://www.apache.org/licenses/LICENSE-2.0
    https://mit-license.org/

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

.. _`DiskStore`: 