# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "diskcache>=5.6.3",
#     "pickledb<1",
#     "sqlitedict",
#     "diskstore",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmarking Key-Value Stores.

Usage:
    uv run scripts/benchmark_kv_store.py
"""

import timeit

import diskcache

import diskstore

N = 100
R = 7
value = "value"


def bench(label, setup, stmt):
    times = timeit.repeat(stmt, setup, number=N, repeat=R, globals=globals())
    best = min(times)
    per_op = best / N
    per_op_str = f"{per_op * 1_000_000:.1f} us" if per_op < 1 else f"{per_op * 1_000:.1f} ms" if per_op < 1 else f"{per_op:.3f} s"
    print(f"  {label:20s} {per_op_str:>12s}  (best of {R}, {N} loops)")


print("diskstore set")
ds = diskstore.DiskStore("/tmp/diskstore_bench_kv.db")
bench("set", "", "ds['key'] = value")
bench("get", "", "ds['key']")
bench("set/delete", "", "ds['key'] = value; del ds['key']")

print("\ndiskcache set")
dc = diskcache.Cache("/tmp/diskcache")
bench("set", "", "dc['key'] = value")
bench("get", "", "dc['key']")
bench("set/delete", "", "dc['key'] = value; del dc['key']")


try:
    import dbm.sqlite3
except ImportError:
    print("Error: Cannot import dbm. Skipping dbm/shelve benchmarks.")
else:
    print("\ndbm set")
    d = dbm.sqlite3.open("/tmp/dbm", "c")
    bench("set", "", "d['key'] = value")
    bench("get", "", "d['key']")
    bench("set/delete", "", "d['key'] = value; del d['key']")

    import shelve

    print("\nshelve set")
    s = shelve.open("/tmp/shelve")
    bench("set", "", "s['key'] = value; s.sync()")
    bench("get", "", "s['key']")
    bench("set/delete", "", "s['key'] = value; s.sync(); del s['key']; s.sync()")

try:
    import dbm.gnu
except ImportError:
    print("Error: Cannot import dbm.gnu. Skipping.")
else:
    print("\ndbm (gnu) set")
    d = dbm.gnu.open("/tmp/dbm", "c")
    bench("set", "", "d['key'] = value; d.sync()")
    bench("get", "", "d['key']")
    bench("set/delete", "", "d['key'] = value; d.sync(); del d['key']; d.sync()")

    import shelve

    print("\nshelve (gnu) set")
    s = shelve.open("/tmp/shelve")
    bench("set", "", "s['key'] = value; s.sync()")
    bench("get", "", "s['key']")
    bench("set/delete", "", "s['key'] = value; s.sync(); del s['key']; s.sync()")

try:
    import sqlitedict
except ImportError:
    print("Error: Cannot import sqlitedict. Skipping.")
else:
    print("\nsqlitedict set")
    sd = sqlitedict.SqliteDict("/tmp/sqlitedict", autocommit=True)
    bench("set", "", "sd['key'] = value")
    bench("get", "", "sd['key']")
    bench("set/delete", "", "sd['key'] = value; del sd['key']")

try:
    import pickledb
except ImportError:
    print("Error: Cannot import pickledb. Skipping.")
else:
    print("\npickledb set")
    p = pickledb.load("/tmp/pickledb", True)
    bench("set", "", "p['key'] = value")
    bench("get", "", "p = pickledb.load('/tmp/pickledb', True); p['key']")
    bench("set/delete", "", "p['key'] = value; del p['key']")
