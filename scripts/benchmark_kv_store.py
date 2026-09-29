# ruff: noqa: E501
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "diskcache>=5.6.3",
#     "pickledb<1",
#     "sqlitedict",
#     "diskstore[apsw]",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmarking Key-Value Stores.

Each store is populated with ``KEYS`` distinct key/value pairs (untimed), then
closed and reopened so SQLite checkpoints the WAL before the timed sections.

Usage:
    uv run scripts/benchmark_kv_store.py
"""

import argparse
import itertools  # noqa: F401  (referenced by timeit setup strings below)
import os
import subprocess
import sys
import timeit

import diskcache

N = 100
R = 7
KEYS = [f"key{i}" for i in range(1000)]
value = "value"
DISKSTORE_DB = "/tmp/diskstore_bench_kv.db"

SETUP = "keys = itertools.cycle(KEYS)"


def bench(label, setup, stmt):
    times = timeit.repeat(stmt, setup, number=N, repeat=R, globals=globals())
    best = min(times)
    per_op = best / N
    per_op_str = f"{per_op * 1_000_000:.1f} us" if per_op < 1 else f"{per_op * 1_000:.1f} ms" if per_op < 1 else f"{per_op:.3f} s"
    print(f"  {label:20s} {per_op_str:>12s}  (best of {R}, {N} loops)")


_args = argparse.ArgumentParser()
_args.add_argument("--diskstore-only", action="store_true", help=argparse.SUPPRESS)
_options = _args.parse_args()

if _options.diskstore_only:
    # Runs in a subprocess so the SQLite backend (chosen at import time via
    # DISKSTORE_BACKEND) can differ between sections.
    import diskstore
    from diskstore import _sqlite

    ds = diskstore.DiskStore(DISKSTORE_DB)
    for key in KEYS:
        ds[key] = value
    ds.close()
    ds.open()
    print(f"\ndiskstore ({_sqlite.BACKEND_NAME})")
    bench("set", SETUP, "ds[next(keys)] = value")
    bench("get", SETUP, "ds[next(keys)]")
    bench("len", "", "len(ds)")
    bench("set/delete", SETUP, "k = next(keys); ds[k] = value; del ds[k]")
    raise SystemExit(0)

for _backend in ("apsw", "sqlite3"):
    _env = dict(os.environ, DISKSTORE_BACKEND=_backend)
    _result = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--diskstore-only"],
        env=_env,
        check=False,
    )
    if _result.returncode != 0:
        print(f"\ndiskstore ({_backend}) skipped (backend not available)")

print("\ndiskcache")
dc = diskcache.Cache("/tmp/diskcache")
for key in KEYS:
    dc[key] = value
dc.close()
dc = diskcache.Cache("/tmp/diskcache")
bench("set", SETUP, "dc[next(keys)] = value")
bench("get", SETUP, "dc[next(keys)]")
bench("len", "", "len(dc)")
bench("set/delete", SETUP, "k = next(keys); dc[k] = value; del dc[k]")


try:
    import dbm.sqlite3
except ImportError:
    print("Error: Cannot import dbm. Skipping dbm/shelve benchmarks.")
else:
    print("\ndbm (sqlite3)")
    d = dbm.sqlite3.open("/tmp/dbm", "c")
    for key in KEYS:
        d[key] = value
    d.close()
    d = dbm.sqlite3.open("/tmp/dbm", "c")
    bench("set", SETUP, "d[next(keys)] = value")
    bench("get", SETUP, "d[next(keys)]")
    bench("len", "", "len(d)")
    bench("set/delete", SETUP, "k = next(keys); d[k] = value; del d[k]")

    import shelve

    print("\nshelve")
    s = shelve.open("/tmp/shelve")
    for key in KEYS:
        s[key] = value
    s.close()
    s = shelve.open("/tmp/shelve")
    bench("set", SETUP, "s[next(keys)] = value; s.sync()")
    bench("get", SETUP, "s[next(keys)]")
    bench("len", "", "len(s)")
    bench("set/delete", SETUP, "k = next(keys); s[k] = value; s.sync(); del s[k]; s.sync()")

try:
    import dbm.gnu
except ImportError:
    print("Error: Cannot import dbm.gnu. Skipping.")
else:
    print("\ndbm (gnu)")
    d = dbm.gnu.open("/tmp/dbm", "c")
    for key in KEYS:
        d[key] = value
    d.sync()
    d.close()
    d = dbm.gnu.open("/tmp/dbm", "c")
    bench("set", SETUP, "d[next(keys)] = value; d.sync()")
    bench("get", SETUP, "d[next(keys)]")
    bench("len", "", "len(d)")
    bench("set/delete", SETUP, "k = next(keys); d[k] = value; d.sync(); del d[k]; d.sync()")

    import shelve

    print("\nshelve (gnu)")
    s = shelve.open("/tmp/shelve")
    for key in KEYS:
        s[key] = value
    s.close()
    s = shelve.open("/tmp/shelve")
    bench("set", SETUP, "s[next(keys)] = value; s.sync()")
    bench("get", SETUP, "s[next(keys)]")
    bench("len", "", "len(s)")
    bench("set/delete", SETUP, "k = next(keys); s[k] = value; s.sync(); del s[k]; s.sync()")

try:
    import sqlitedict
except ImportError:
    print("Error: Cannot import sqlitedict. Skipping.")
else:
    print("\nsqlitedict")
    sd = sqlitedict.SqliteDict("/tmp/sqlitedict", autocommit=True)
    for key in KEYS:
        sd[key] = value
    sd.close()
    sd = sqlitedict.SqliteDict("/tmp/sqlitedict", autocommit=True)
    bench("set", SETUP, "sd[next(keys)] = value")
    bench("get", SETUP, "sd[next(keys)]")
    bench("len", "", "len(sd)")
    bench("set/delete", SETUP, "k = next(keys); sd[k] = value; del sd[k]")

try:
    import pickledb
except ImportError:
    print("Error: Cannot import pickledb. Skipping.")
else:
    print("\npickledb")
    p = pickledb.load("/tmp/pickledb", True)
    for key in KEYS:
        p[key] = value
    p.dump()
    bench("set", SETUP, "p[next(keys)] = value")
    bench("get", SETUP, "p = pickledb.load('/tmp/pickledb', True); p[next(keys)]")
    bench("len", "", "p.totalkeys()")
    bench("set/delete", SETUP, "k = next(keys); p[k] = value; del p[k]")
