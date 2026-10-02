# Changelog

## Unreleased

### Added
- `scripts/busy_load.py` — multi-process load test asserting that every busy condition surfaces as `BusyError` rather than a raw driver error; run it with `--backend apsw|sqlite3`

### Fixed
- The stdlib backend no longer leaks a file descriptor per abandoned thread. Connections were created via a `ClosingConnection` subclass that closed itself in `__del__`, but a finalizer runs on the GC thread and sqlite3 refuses to close a connection created by another thread, so the `close()` raised `ProgrammingError` and the handle was never released. Since DiskStore keeps connections in thread-local state, every thread that ended without calling `close()` leaked a descriptor for the life of the process. `connect()` now returns a plain `sqlite3.Connection`, whose handle is reclaimed with the object, matching the APSW backend
- `is_busy()` now recognises a re-raised `BusyError` on the APSW backend; the diskstore `BusyError` subclasses `apsw.BusyError` rather than `apsw.Error`, so a `while is_busy(exc): retry` loop no longer spins forever when APSW is active
- `clear()`, `check(vacuum=True)`, `_migrate_table()`, `set_pragma()` and `table_columns()` now raise `BusyError` under contention instead of leaking the raw driver error. All of these ran on connection setup or on write paths while bypassing the busy translation in `execute()`
- A failed `auto_migrate` migration no longer leaves the connection inside a transaction. `_migrate_table()` opened `BEGIN IMMEDIATE` without a rollback on error, so a failure part-way through adding columns kept the write lock held and blocked every other writer to that database until the connection was closed
- Corrected the `is_busy()` docstring on the APSW backend, which wrongly claimed APSW exposes no error code. The code is available as `exc.extendedresult` (`exc.result` for the primary code), the counterpart to the stdlib backend's `sqlite_errorname`, and both drivers report `SQLITE_BUSY_SNAPSHOT` correctly

### Changed
- The stdlib backend's `BusyError` now derives from `sqlite3.Error` rather than `sqlite3.OperationalError`, mirroring the apsw shape where `apsw.BusyError` sits directly under `apsw.Error`. Note `SQLError` *is* `sqlite3.Error` on that backend, so a `BusyError` remains a `SQLError` there while it is not one under apsw; catch `(BusyError, SQLError)` to cover both backends
- A `DiskStore` whose connection is garbage collected without `close()` now emits sqlite3's `ResourceWarning: unclosed database`, where the removed `__del__` finalizer used to hide it. The warning is the intended signal that `close()` was missed; it is raised during finalization, so it prints but cannot fail a test. Store the object and call `close()` (or use it as a context manager) to avoid it

## 0.6.0 (2026-10-01)

### Added
- Optional APSW accelerator: when `apsw` is installed it is used automatically; install with `diskstore[apsw]`. Force a backend with `DISKSTORE_BACKEND=apsw|sqlite3`.
- PEP 561 `py.typed` marker, so type checkers use the inline annotations

### Changed
- SQLite driver defaults to the Python standard-library `sqlite3` module, so there is no required third-party runtime dependency
- The stdlib path requires SQLite >= 3.35 (for `INSERT ... RETURNING`); APSW bundles its own SQLite
- `apsw` moved to an optional extra and the `dev` dependency group (used by the A/B benchmark)
- `BusyError` is now a diskstore-owned exception raised by both backends; other driver errors are backend-native and re-exported as `SQLError`/`Error`
- `transact()` yields the active backend's cursor (`sqlite3.Cursor` or `apsw.Cursor`)
- Private `_con` is the active backend's connection; PRAGMA access goes through internal helpers

## 0.5.0 (2026-09-20)

### Added
- `page_size` default of 16 KB — fewer B-tree pages and a smaller database for larger values
- `wal_autocheckpoint` default of 5000 pages — fewer, larger WAL checkpoints
- `__contains__` uses `SELECT 1` instead of selecting the key

### Changed
- `__len__` uses `COUNT(*)`, letting SQLite use its specialised b-tree count (up to ~150× faster for text keys)
- `cache_size` default expressed as 32 MB (negative value, KiB) so it is independent of `page_size`
- Transaction state is per-thread (`_ThreadState`); `transact()` no longer shares state between threads
- `__delitem__` reuses the per-thread cursor and drains the statement with `fetchall()`

### Fixed
- A fork inside a transaction no longer inherits transaction state, so the child rolls back correctly
- `__delitem__` no longer leaves a statement in progress, which could suppress WAL autocheckpoint or block a later `COMMIT`

## 0.4.0 (2026-09-13)

### Added
- `StructtypeConfig` — optional config class backed by `structtype` for fast, schema-validated serialization
- Python 3.15 test coverage

### Changed
- Documentation overhaul with self-contained examples
- Relicensed from Apache-2.0 OR MIT to BSD-3-Clause

### Removed
- `scripts/test_diskcache.py`

## 0.3.1 (2026-06-05)

### Added
- `DiskStore._migrate_table()` creates the table if absent and adds missing columns via `ALTER TABLE ADD COLUMN` — no destructive migrations
- `DiskStore` now calls `_migrate_table()` on first connection per-process, enabling auto-migration without explicit setup
- Nullable column support via `NO_DEFAULT` sentinel — columns with no default can be omitted from INSERT
- `DiskRead` now applies pragmas (e.g. mmap) for read-only connections
- Default timeout reduced from 30s to 10s for both `DiskStore` and `DiskRead`
- Bool-to-INTEGER SQLite type coercion in `DataclassConfig`
- `make update-python` command for changing `.python-version`

### Changed
- `dump_value()` now receives the key as the first argument and returns a sequence with key first — more flexibility for custom configs
- `load_data()` now receives the key at position 0 of the row tuple — less tuple repacking
- `store.update()`, `pop()`, `popitem()` simplified and streamlined
- Internal `_cursor` property removed; all operations use the connection directly
- `expandvars` support in filename removed (unused, adds complexity)
- `limit` and `offset` in `DiskRead.query()` are now always coerced to `int`

### Fixed
- Race condition in `popitem()` — re-reads after acquiring connection
- Race condition in `setdefault()` — race-free INSERT or SELECT pattern
- Pytest warnings resolved; test stability improved

## 0.2.0 (2026-03-31)

### Added
- Config class system: `BaseConfig`, `NamedTupleConfig`, `JsonConfig`, `DataclassConfig`, `PydanticConfig`
- `dump`/`load` serializer architecture — each config class controls how Python values are serialized to/from SQLite columns
- `DiskRead.query()` with `where`, `parameters`, `order`, `limit`, `offset`
- `DiskRead.open()` / `close()` methods for explicit connection lifecycle
- Context manager support (`__enter__` / `__exit__`) on `DiskRead`
- Pragma configuration in config objects (WAL, mmap, cache, synchronous)
- `DiskKeysView`, `DiskValuesView`, `DiskItemsView` — custom view classes

### Fixed
- Pragma settings now actually applied on connection creation

### Removed
- Windows CI testing — macOS and Linux only

## 0.1.0 (2026-02-08)

### Added
- Initial release
- `DiskStore` — read-write `MutableMapping` with SQLite backend
- `DiskRead` — read-only `Mapping` with SQLite backend
- WAL journal mode, 256MB mmap, `synchronous=NORMAL`, 8192-page cache
- Per-thread connection pooling with `threading.local()` and fork detection via `os.getpid()`
- `BEGIN IMMEDIATE` transactions to avoid deadlocks
