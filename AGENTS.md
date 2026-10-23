# diskstore — agent guide

Single-package Python library providing fast SQLite-backed `MutableMapping`/`Mapping` storage.

## Toolchain

- **Build/package**: `uv` (not pip/poetry). Lockfile: `uv.lock`. Builder: `hatchling` + `uv-dynamic-versioning`.
- **Runtime dep**: none — uses the stdlib `sqlite3` module; if `apsw` is installed it is used automatically (`diskstore[apsw]`). The stdlib path requires SQLite >= 3.35 for `RETURNING`. `apsw` is also a dev dependency so tests run against both backends.
- **Lint**: `uvx ruff check src/` (config in `pyproject.toml` — includes `ruff` lints + `isort`, `flake8-bugbear`, `flake8-pytest`, `pylint`, `naming`)
- **Format**: `uvx ruff format src/`
- **Type check**: `uvx ty check src/` (`ty` call has `-` prefix in `make check` — its exit code is ignored). `make check` runs ty + ruff (lint); `make type-check` runs ty only.
- **Docs**: `zensical` (mkdocs-compatible), not plain mkdocs. Build with `uv run --group docs zensical build`
- **Python**: `>=3.10`. CI tests:
  - `make test-all`: 3.10–3.15 (stable releases)
  - `make test-latest`: 3.14t + 3.15t (free-threaded), both backends

## Commands

| `make test` | run tests against both sqlite drivers (`sqlite3` then `apsw`) |
| `make test-lf` | `uv run pytest --lf -n auto` (last-failed, parallel) |
| `make test-cov` | pytest with coverage (used by CI) |
| `make test-all` | run tests across all supported Python versions, both backends |
| `make test-latest` | 3.14t + 3.15t (free-threaded), both backends |
| `make check` | lint (ruff) + type check (ty) |
| `make type-check` | type check (ty) |
| `make ruff-check` | lint only (ruff) |
| `make format` | ruff format |
| `make build` | `uv build` |
| `make docs` | build docs via zensical |
| `make bench` | `uv run scripts/benchmark_core.py -p 1` |
| `make bench-kv` | `uv run scripts/benchmark_kv_store.py` |
| `make bench-all` | `uv run scripts/benchmark.py` |
| `make bench-configs` | `uv run scripts/benchmark_configs.py` (BaseConfig vs StructtypeConfig vs PydanticConfig) |
| `make bench-ab` | `uv run scripts/bench_ab.py` (apsw vs stdlib sqlite3) |
| `make install` | `uv sync --frozen` |
| `make update-uv` | `uv self update` |
| `make update-lock` | `uv lock --upgrade` |
| `make update-python` | reinstall managed Python versions |
| `make clean` | remove all caches and build artifacts |

### Focused test commands

```sh
uv run pytest tests/test_diskstore.py -n auto
uv run pytest tests/test_diskstore.py::test_getsetdel -xvs
uv run pytest tests/test_config.py -n auto
uv run pytest tests/test_diskread.py -n auto
uv run pytest tests/test_diskstore_classes.py -n auto -k "Msgspec or Pydantic"
```

## Architecture

- `src/diskstore/diskstore.py` → `DiskStore` (read-write, `MutableMapping`)
- `src/diskstore/diskread.py` → `DiskRead` (read-only, `Mapping`)
- `src/diskstore/config.py` → `BaseConfig`, `NamedTupleConfig`, `JsonConfig`, `DataclassConfig`, `PydanticConfig`, `StructtypeConfig`
- `src/diskstore/const.py` → defaults (WAL journal, 16KB page size, 32MB cache, 256MB mmap, synchronous=NORMAL, wal_autocheckpoint=5000)
- `src/diskstore/_sqlite.py` → SQLite backend selector (apsw when installed, else stdlib) + shared busy translation; `BusyError` is re-exported from the package root
- `src/diskstore/_sqlite_stdlib.py` / `_sqlite_apsw.py` → per-driver backends; `_sqlite_common.py` → shared SQL literal helpers

## Quirks

- **`_con` property** lazy-creates connections per-thread (`threading.local()`). Detects forks via an `os.register_at_fork` generation counter (`os.getpid()` fallback) and reconnects.
- **`close_all()`** closes every thread's connection, not just the caller's, and bumps a close generation so each thread rebuilds on next access. The stdlib backend opens with `check_same_thread=False` so a connection can be closed from another thread; this is still a teardown-only operation.
- **Transactions** use `BEGIN IMMEDIATE` (not DEFERRED) to avoid deadlocks. Nested `transact()` calls are idempotent.
- **`DiskStore(..., config=BaseConfig(key_type=int))`** allows auto-increment via `store.add(None, value)` (SQLite INTEGER PRIMARY KEY NULL → rowid).
- **`_migrate_table()`** creates the table if absent and adds missing columns via `ALTER TABLE ADD COLUMN` — no destructive migrations.
- **Benchmark scripts** use PEP 723 inline script metadata (see `scripts/benchmark.py`, `benchmark_core.py`, `benchmark_kv_store.py`, `bench_ab.py`, `benchmark_configs.py`).
- **Tests with pytest.mark.skipif**: `TestMsgspecStruct`, `TestPydanticModel`, `TestStructtypeStruct`, `TestPydanticConfig`, `TestStructtypeConfig` — conditional on optional deps.
- **`test_docs.py`** uses `pytest-examples` to validate the fenced code examples in `docs/index.md`. Run separately if docs fail.

## Rules

- run test after changes
- format code with ruff
- no commits
