# diskstore — agent guide

Single-package Python library providing fast SQLite-backed `MutableMapping`/`Mapping` storage.

## Toolchain

- **Build/package**: `uv` (not pip/poetry). Lockfile: `uv.lock`. Builder: `uv_build`.
- **Runtime dep**: `apsw` (not stdlib `sqlite3`).
- **Lint**: `uvx ruff check src/` (config in `pyproject.toml` — includes `ruff` lints + `isort`, `flake8-bugbear`, `flake8-pytest`, `pylint`, `naming`)
- **Format**: `uvx ruff format src/`
- **Type check**: `uvx ty check src/` then `uvx pyrefly check src/` (`ty` call has `-` prefix — its exit code is ignored)
- **Docs**: `zensical` (mkdocs-compatible), not plain mkdocs. Build with `uv run --group docs zensical build`
- **Python**: `>=3.10`, default 3.14 (`.python-version`). CI tests 3.10–3.15 including free-threaded (`3.14t`) and pre-release (`3.15`).

## Commands

| `make test` | `uv run pytest --lf -n auto` (last-failed, parallel) |
| `make cov` | pytest with coverage (used by CI) |
| `make tests` | run tests across all supported Python versions |
| `make check` | lint + type check |
| `make format` | ruff format |
| `make docs` | build docs via zensical |
| `make bench` | run benchmarks |
| `make install` | `uv sync --frozen` |
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
- `src/diskstore/config.py` → `BaseConfig`, `NamedTupleConfig`, `JsonConfig`, `DataclassConfig`, `PydanticConfig`
- `src/diskstore/const.py` → defaults (WAL journal, 256MB mmap, synchronous=NORMAL, cache=8192 pages)

## Quirks

- **`_con` property** lazy-creates connections per-thread (`threading.local()`). Detects process forks via `os.getpid()` and reconnects.
- **Transactions** use `BEGIN IMMEDIATE` (not DEFERRED) to avoid deadlocks. Nested `transact()` calls are idempotent.
- **`DiskStore(key_type=int)`** allows auto-increment via `store.add(None, value)` (SQLite INTEGER PRIMARY KEY NULL → rowid).
- **`_migrate_table()`** adds columns via `ALTER TABLE ADD COLUMN` — no destructive migrations.
- **Benchmark scripts** use PEP 723 inline script metadata (see `scripts/benchmark_core.py`).
- **Tests with pytest.mark.skipif**: `TestMsgspecStruct`, `TestPydanticModel` — conditional on optional deps.
- **`test_docs.py`** uses `pytest-examples` to validate docstring examples in `docs/index.md`. Run separately if docs fail.
- **Editor**: `.vscode/settings.json` exists — check before overriding.
