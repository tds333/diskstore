# Changelog

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
