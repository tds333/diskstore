# diskstore — agent guide

Single-package Python library providing fast SQLite-backed `MutableMapping`/`Mapping` storage.

## Toolchain

- **Build/package**: `uv` (not pip/poetry). Lockfile: `uv.lock`. Builder: `uv_build`.
- **Runtime dep**: `apsw` (not stdlib `sqlite3`).
- **Lint**: `uvx ruff check src/` (config in `pyproject.toml` — includes `ruff` lints + `isort`, `flake8-bugbear`, `flake8-pytest`, `pylint`, `naming`)
- **Format**: `uvx ruff format src/`
- **Type check**: `uvx ty check src/` then `uvx pyrefly check src/` (`ty` call has `-` prefix — its exit code is ignored). `make check` runs ty + ruff (lint); `make type-check` runs ty + pyrefly.
- **Docs**: `zensical` (mkdocs-compatible), not plain mkdocs. Build with `uv run --group docs zensical build`
- **Python**: `>=3.10`, default 3.14 (`.python-version`). CI tests:
  - `make tests`: 3.10–3.14 (stable releases)
  - `make latest-tests`: 3.14t (free-threaded), 3.15 (pre-release), 3.15t (pre-release free-threaded)

## Commands

| `make test` | `uv run pytest --lf -n auto` (last-failed, parallel) |
| `make cov` | pytest with coverage (used by CI) |
| `make tests` | run tests across all supported Python versions |
| `make latest-tests` | 3.14t + 3.15 + 3.15t (free-threaded/pre-release) |
| `make check` | lint (ruff) + type check (ty) |
| `make type-check` | type check (ty + pyrefly) |
| `make ruff-check` | lint only (ruff) |
| `make format` | ruff format |
| `make build` | `uv build` |
| `make docs` | build docs via zensical |
| `make bench` | `uv run scripts/benchmark_core.py -p 1` |
| `make bench-kv` | `uv run scripts/benchmark_kv_store.py` |
| `make bench-all` | `uv run scripts/benchmark.py` |
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
- `src/diskstore/config.py` → `BaseConfig`, `NamedTupleConfig`, `JsonConfig`, `DataclassConfig`, `PydanticConfig`
- `src/diskstore/const.py` → defaults (WAL journal, 256MB mmap, synchronous=NORMAL, cache=8192 pages)

## Quirks

- **`_con` property** lazy-creates connections per-thread (`threading.local()`). Detects process forks via `os.getpid()` and reconnects.
- **Transactions** use `BEGIN IMMEDIATE` (not DEFERRED) to avoid deadlocks. Nested `transact()` calls are idempotent.
- **`DiskStore(key_type=int)`** allows auto-increment via `store.add(None, value)` (SQLite INTEGER PRIMARY KEY NULL → rowid).
- **`_migrate_table()`** creates the table if absent and adds missing columns via `ALTER TABLE ADD COLUMN` — no destructive migrations.
- **Benchmark scripts** use PEP 723 inline script metadata (see `scripts/benchmark_core.py`, `scripts/benchmark_kv_store.py`, `scripts/benchmark.py`).
- **Tests with pytest.mark.skipif**: `TestMsgspecStruct`, `TestPydanticModel` — conditional on optional deps.
- **`test_docs.py`** uses `pytest-examples` to validate docstring examples in `docs/index.md`. Run separately if docs fail.

## Rules

- run test after changes
- format code with ruff

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, use the installed graphify skill or instructions before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
