from time import time
from typing import NamedTuple

from diskstore import DiskStore, Value


class LotColumns(NamedTuple):
    value: str
    i1: int = 1
    i2: int = 2
    i3: int = 3
    i4: int = 4
    i5: int = 5
    i6: int = 6
    i7: int = 7
    i8: int = 8
    i9: int = 9


def main():
    amount = 1_000_000
    ds = DiskStore("/tmp/bigfile.db")
    ds2 = DiskStore("/tmp/bigfile2.db", value_class=LotColumns)
    ds3 = DiskStore("/tmp/bigfile.db", tablename="Second")

    ds.clear()
    ds.check(vacuum=True)
    ds2.clear()
    ds2.check(vacuum=True)
    ds3.clear()
    ds3.check(vacuum=True)

    # ds["start"] = "with äü@"

    t0 = time()
    for i in range(amount):
        ds[f"shard{i}"] = Value(str(i) * 100)
    t1 = time()
    duration = t1 - t0
    print(f"duration: {duration:.2f}s")
    ds2.update((i, LotColumns(str(i) * 100, i, i % 100)) for i in range(amount))
    t2 = time()
    duration = t2 - t1
    print(f"duration fast: {duration:.2f}s")
    ds3.update(((i, Value(str(i) * 100)) for i in range(amount)))
    t3 = time()
    duration = t3 - t2
    print(f"duration fastest: {duration:.2f}s")
    t4 = time()
    result = list(ds2.query(where="i2=10"))
    print(len(result))
    duration = t4 - t3
    print(f"find duration: {duration:.2f}s")
    ds.close()
    ds2.close()
    ds3.close()


def main_find():
    amount = 1_000_000
    ds = DiskStore("/tmp/bigfile2.db", value_class=LotColumns)
    if len(ds) != amount:
        ds.clear()
        ds.check(vacuum=True)

        t1 = time()
        ds.update((i, LotColumns(str(i) * 100, i, i % 1000)) for i in range(amount))
        t2 = time()
        duration = t2 - t1
        print(f"duration init: {duration:.2f}s")
    tablename = ds.tablename
    with ds.transact() as cx:
        cx.execute(f"CREATE INDEX IF NOT EXISTS idx_LotColumns_i2 ON {tablename}(i2);")
    t3 = time()
    result = list(ds.query(where="i2=10"))
    t4 = time()
    print("found:", len(result))
    duration = t4 - t3
    print(f"find duration: {duration:.5f}s")
    ds.close()


if __name__ == "__main__":
    # main()
    main_find()
