"""Benchmarking Key-Value Stores

$ python -m IPython tests/benchmark_kv_store.py
"""

import diskcache
from IPython import get_ipython

import diskstore

ipython = get_ipython()
assert ipython is not None, "No IPython! Run with $ ipython ..."

value = "value"

print("diskstore set")
dc = diskstore.DiskStore("/tmp/diskstore_bench_kv.db")
Value = diskstore.Value
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key'] = Value(value)")
print("diskstore get")
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key']")
print("diskstore set/delete")
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key'] = Value(value); del dc['key']")


print("\ndiskcache set")
dc = diskcache.Cache("/tmp/diskcache")
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key'] = value")
print("diskcache get")
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key']")
print("diskcache set/delete")
ipython.run_line_magic("timeit", "-n 100 -r 7 dc['key'] = value; del dc['key']")

try:
    import dbm.sqlite3  # Only trust GNU DBM
except ImportError:
    print("Error: Cannot import dbm.")
    print("Error: Skipping import shelve")
else:
    print("\ndbm set")
    d = dbm.sqlite3.open("/tmp/dbm", "c")
    ipython.run_line_magic("timeit", "-n 100 -r 7 d['key'] = value")
    print("dbm get")
    ipython.run_line_magic("timeit", "-n 100 -r 7 d['key']")
    print("dbm set/delete")
    ipython.run_line_magic("timeit", "-n 100 -r 7 d['key'] = value; del d['key']")

    import shelve

    print("\nshelve set")
    s = shelve.open("/tmp/shelve")
    ipython.run_line_magic("timeit", "-n 100 -r 7 s['key'] = value; s.sync()")
    print("shelve get")
    ipython.run_line_magic("timeit", "-n 100 -r 7 s['key']")
    print("shelve set/delete")
    ipython.run_line_magic(
        "timeit", "-n 100 -r 7 s['key'] = value; s.sync(); del s['key']; s.sync()"
    )

try:
    import dbm.gnu  # Only trust GNU DBM
except ImportError:
    print("Error: Cannot import dbm.gnu")
    print("Error: Skipping import shelve")
else:
    print("\ndbm set")
    d = dbm.gnu.open("/tmp/dbm", "c")
    ipython.run_line_magic("timeit", "-n 100 -r 7 d['key'] = value; d.sync()")
    print("dbm get")
    ipython.run_line_magic("timeit", "-n 100 -r 7 d['key']")
    print("dbm set/delete")
    ipython.run_line_magic(
        "timeit", "-n 100 -r 7 d['key'] = value; d.sync(); del d['key']; d.sync()"
    )

    import shelve

    print("\nshelve set")
    s = shelve.open("/tmp/shelve")
    ipython.run_line_magic("timeit", "-n 100 -r 7 s['key'] = value; s.sync()")
    print("shelve get")
    ipython.run_line_magic("timeit", "-n 100 -r 7 s['key']")
    print("shelve set/delete")
    ipython.run_line_magic(
        "timeit", "-n 100 -r 7 s['key'] = value; s.sync(); del s['key']; s.sync()"
    )

try:
    import sqlitedict
except ImportError:
    print("Error: Cannot import sqlitedict")
else:
    print("\nsqlitedict set")
    sd = sqlitedict.SqliteDict("/tmp/sqlitedict", autocommit=True)
    ipython.run_line_magic("timeit", "-n 100 -r 7 sd['key'] = value")
    print("sqlitedict get")
    ipython.run_line_magic("timeit", "-n 100 -r 7 sd['key']")
    print("sqlitedict set/delete")
    ipython.run_line_magic("timeit", "-n 100 -r 7 sd['key'] = value; del sd['key']")

try:
    import pickledb
except ImportError:
    print("Error: Cannot import pickledb")
else:
    print("\npickledb set")
    p = pickledb.load("/tmp/pickledb", True)
    ipython.run_line_magic("timeit", "-n 100 -r 7 p['key'] = value")
    print("pickledb get")
    ipython.run_line_magic(
        "timeit", "-n 100 -r 7 p = pickledb.load('/tmp/pickledb', True); p['key']"
    )
    print("pickledb set/delete")
    ipython.run_line_magic("timeit", "-n 100 -r 7 p['key'] = value; del p['key']")
