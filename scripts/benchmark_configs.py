# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "diskstore",
#     "pydantic>=2.12.5",
#     "structtype>=0.3.0",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmark diskstore config classes: BaseConfig vs StructtypeConfig vs PydanticConfig.

Compares per-op set/get/delete cost of the same logical JSON payload under each
config, isolating (de)serialization overhead on top of identical SQLite I/O.

Usage:
    uv run scripts/benchmark_configs.py
    uv run scripts/benchmark_configs.py --ops 5000 --rounds 3
"""

import argparse
import json
import os
import platform
import random
import shutil
import sys
import tempfile
import time
from importlib.metadata import PackageNotFoundError, version

import pydantic
import structtype

from diskstore import DiskStore
from diskstore.config import BaseConfig, PydanticConfig, StructtypeConfig

OPS = 10_000
DBSIZE = 10_000
ROUNDS = 3
SEED = 42


def make_product(i: int) -> dict:
    return {
        "id": f"p{i}",
        "name": f"product-{i}",
        "price": i * 1.5,
        "stock": i,
        "tags": ["alpha", "beta", "gamma"],
    }


class Product(structtype.Struct):
    id: str
    name: str
    price: float
    stock: int
    tags: list[str]


class ProductModel(pydantic.BaseModel):
    id: str
    name: str
    price: float
    stock: int
    tags: list[str]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


def compute_stats(values: list[float]) -> dict[str, float]:
    sv = sorted(values)
    n = len(sv)
    return {
        "count": n,
        "median": sv[n // 2],
        "p90": sv[int(n * 0.9)],
        "p99": sv[int(n * 0.99)],
        "max": sv[-1],
        "total": sum(values),
    }


def fmt_secs(value: float) -> str:
    units = [("s ", 1), ("ms", 1e-3), ("us", 1e-6), ("ns", 1e-9)]
    if value == 0:
        return "  0.000ns"
    v = abs(value)
    for suffix, threshold in units:
        if v >= threshold:
            return "%7.3f" % (value / threshold) + suffix
    return "%7.3f" % (value / 1e-9) + "ns"


def runtime_info() -> dict:
    try:
        dv = version("diskstore")
    except PackageNotFoundError:
        dv = "unknown"
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "diskstore": dv,
    }


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


class ConfigBenchmark:
    BATCH = 500

    def __init__(self, tmpdir: str, ops: int, dbsize: int, rounds: int):
        self.tmpdir = tmpdir
        self.ops = ops
        self.dbsize = dbsize
        self.rounds = rounds
        self.summary: dict[str, dict] = {}

    def bench(self, label: str, fn, n: int | None = None):
        n = n or self.ops
        wall_start = time.perf_counter()
        per_op: list[float] = []
        full = n // self.BATCH
        rem = n % self.BATCH
        if full:
            for _ in range(full):
                start = time.perf_counter()
                for _ in range(self.BATCH):
                    fn()
                per_op.append((time.perf_counter() - start) / self.BATCH)
        if rem:
            start = time.perf_counter()
            for _ in range(rem):
                fn()
            per_op.append((time.perf_counter() - start) / rem)
        total = time.perf_counter() - wall_start
        stats = compute_stats(per_op)
        stats["count"] = n
        stats["total"] = total
        self.summary[label] = stats

    def _run_once(self, rundir: str):
        keys = [f"k{i}" for i in range(self.dbsize)]
        target_keys = [random.choice(keys) for _ in range(self.ops)]
        del_keys = [f"del{i}" for i in range(self.ops)]

        configs = [
            ("BaseConfig", BaseConfig(), json.dumps(make_product(0))),
            ("StructtypeConfig", StructtypeConfig(Product), Product(**make_product(0))),
            (
                "PydanticConfig",
                PydanticConfig(ProductModel),
                ProductModel(**make_product(0)),
            ),
        ]

        for label, config, value in configs:
            self._bench_store(rundir, label, config, value, keys, target_keys, del_keys)

    def _bench_store(  # noqa: PLR0913, PLR0917
        self,
        rundir: str,
        label: str,
        config,
        value,
        keys: list[str],
        target_keys: list[str],
        del_keys: list[str],
    ):
        store = DiskStore(os.path.join(rundir, f"{label}.db"), config)
        store.open()
        store.update(dict.fromkeys(keys, value))

        for k in keys[:100]:
            _ = store.get(k)

        i = 0

        def fn_set():
            nonlocal i
            store[target_keys[i % len(target_keys)]] = value
            i += 1

        self.bench(f"set [{label}]", fn_set)

        j = 0

        def fn_get():
            nonlocal j
            _ = store[target_keys[j % len(target_keys)]]
            j += 1

        self.bench(f"get [{label}]", fn_get)

        k = 0

        def fn_del():
            nonlocal k
            try:
                del store[del_keys[k]]
            except KeyError:
                pass
            k += 1

        self.bench(f"delete [{label}]", fn_del)

        store.close()

    def run_all(self):
        round_stats: list[dict] = []
        for r in range(self.rounds):
            self.summary = {}
            rundir = os.path.join(self.tmpdir, f"round_{r}")
            os.makedirs(rundir)
            self._run_once(rundir)
            round_stats.append(dict(self.summary))
            if self.rounds > 1:
                print(f"  round {r + 1}/{self.rounds} done")

        labels = round_stats[0].keys()
        final = {}
        for label in labels:
            medians = [rs[label]["median"] for rs in round_stats]
            totals = [rs[label]["total"] for rs in round_stats]
            medians.sort()
            totals.sort()
            final[label] = {
                "count": round_stats[0][label]["count"],
                "median": medians[len(medians) // 2],
                "total": totals[len(totals) // 2],
            }
        self.summary = final

    def display(self):
        ops = ("set", "get", "delete")
        configs = ("BaseConfig", "StructtypeConfig", "PydanticConfig")
        widths = max(len(c) for c in configs) + 2
        cols = 20 + 3 * widths
        count = self.summary.get(f"set [{configs[0]}]", {}).get("count", "-")
        print()
        print("=" * cols)
        print(f"  RESULTS  ({count} ops per operation, median over rounds)")
        print("=" * cols)
        print(f"  {'Operation':<14}{'':4}" + "".join(f"{c:>{widths}}" for c in configs))
        print("-" * cols)
        for op in ops:
            base = self.summary.get(f"{op} [{configs[0]}]", {}).get("median")
            cells = []
            totals = []
            for c in configs:
                s = self.summary.get(f"{op} [{c}]", {})
                median = s.get("median")
                if median is None:
                    cells.append(" " * widths)
                    totals.append(" " * widths)
                    continue
                cell = fmt_secs(median)
                if base and c != configs[0]:
                    cell += " (%+.1f%%)" % ((median - base) / base * 100)
                cells.append(f"{cell:>{widths}}")
                totals.append(f"{fmt_secs(s.get('total', 0)):>{widths}}")
            print(f"  {op:<14}{'':4}" + "".join(cells))
            print(f"  {'total':<14}{'':4}" + "".join(totals))
        print("=" * cols)


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark diskstore config classes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ops", type=int, default=OPS)
    parser.add_argument("--dbsize", type=int, default=DBSIZE, help="DB item count")
    parser.add_argument("--rounds", type=int, default=ROUNDS, help="Benchmark rounds")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    random.seed(args.seed)

    rt = runtime_info()
    print(f"Ops per op:       {args.ops}")
    print(f"DB size:          {args.dbsize} items")
    print(f"Rounds:           {args.rounds}")
    print(f"Python:           {rt['python']}")
    print(f"Platform:         {rt['platform']}")
    print(f"diskstore:        {rt['diskstore']}")

    tmpdir = tempfile.mkdtemp(prefix="diskstore_bench_configs-")
    bm = ConfigBenchmark(
        tmpdir=tmpdir,
        ops=args.ops,
        dbsize=args.dbsize,
        rounds=args.rounds,
    )
    try:
        bm.run_all()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    bm.display()


if __name__ == "__main__":
    main()
