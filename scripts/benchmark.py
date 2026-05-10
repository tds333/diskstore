#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "diskstore",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmark get, set, delete, update with historical comparison.

Usage:
    uv run scripts/benchmark.py
    uv run scripts/benchmark.py --ops 5000 --sizes 1000 10000
    uv run scripts/benchmark.py --results-dir /tmp/my-bench
"""

import argparse
import json
import os
import random
import shutil
import string
import tempfile
import time

from diskstore import DiskStore

OPS = 10_000
SEED = 42


def make_value(size: int) -> bytes:
    return random.choice(string.ascii_letters).encode() * size


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

BENCH_COLS = ("count", "median", "total")
BENCH_HEADERS = ("Count", "Median", "Total")


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


def fmt_pct(diff: float) -> str:
    if diff is None or diff == 0:
        return "    0.0%"
    return "%+7.1f%%" % (diff * 100)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class ResultsStore:
    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def _last_path(self) -> str:
        return os.path.join(self.directory, "last.json")

    def load_last(self) -> dict | None:
        path = self._last_path()
        if not os.path.exists(path):
            return None
        with open(path) as f:
            return json.load(f)

    def save(self, stats: dict, args: dict):
        ts = time.strftime("%Y%m%dT%H%M%S")
        payload = {"timestamp": ts, "args": args, "stats": stats}
        filename = f"bench_{ts}.json"
        with open(os.path.join(self.directory, filename), "w") as f:
            json.dump(payload, f, indent=2)
        last = self._last_path()
        if os.path.exists(last) or os.path.islink(last):
            os.remove(last)
        with open(last, "w") as f:
            json.dump(payload, f, indent=2)


# ---------------------------------------------------------------------------
# Deltas
# ---------------------------------------------------------------------------

def compute_deltas(
    current: dict, previous: dict, *, stat_keys: tuple = ("median", "total")
) -> dict[str, dict]:
    deltas: dict[str, dict] = {}
    for label, cur_stats in current.items():
        prev_stats = previous.get(label)
        if prev_stats is None:
            continue
        d = {}
        for sk in stat_keys:
            cv = cur_stats.get(sk)
            pv = prev_stats.get(sk)
            if cv is not None and pv and pv != 0:
                d[sk] = (cv - pv) / pv
            else:
                d[sk] = None
        deltas[label] = d
    return deltas


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

class Benchmark:
    BATCH = 500

    def __init__(self, tmpdir: str, ops: int, valsize: int, dbsize: int, rounds: int):
        self.tmpdir = tmpdir
        self.ops = ops
        self.valsize = valsize
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

    def _run_once(self, dbpath: str):
        value = make_value(self.valsize)
        store = DiskStore(dbpath)

        keys = [f"k{i}" for i in range(self.dbsize)]
        store.update(dict.fromkeys(keys, value))

        for k in keys[:100]:
            _ = store.get(k)

        target_keys = [random.choice(keys) for _ in range(self.ops)]
        tag = f"val={self.valsize}B  db={self.dbsize}"

        i = 0
        def fn_set():
            nonlocal i
            store[target_keys[i % len(target_keys)]] = value
            i += 1
        self.bench(f"set [{tag}]", fn_set)

        j = 0
        def fn_get():
            nonlocal j
            _ = store[target_keys[j % len(target_keys)]]
            j += 1
        self.bench(f"get [{tag}]", fn_get)

        del_keys = [f"del{i}" for i in range(self.ops)]
        store.update(dict.fromkeys(del_keys, value))
        k = 0
        def fn_del():
            nonlocal k
            try:
                del store[del_keys[k]]
            except KeyError:
                pass
            k += 1
        self.bench(f"delete [{tag}]", fn_del)

        bulk_n = min(self.ops, 5000)
        bulk_data = {f"bulk{i}": make_value(self.valsize) for i in range(bulk_n)}
        def fn_update():
            store.update(bulk_data)
        self.bench(f"update [{tag}]  n={bulk_n}", fn_update, n=1)

        store.close()

    def run_all(self):
        round_stats: list[dict] = []
        for r in range(self.rounds):
            self.summary = {}
            dbpath = os.path.join(self.tmpdir, f"bench_{r}.db")
            self._run_once(dbpath)
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

    # ── Display ────────────────────────────────────────────────

    COLS = 71

    def display_single(self):
        print()
        print("=" * self.COLS)
        print("  RESULTS")
        print("=" * self.COLS)
        t = "  %-36s %8s  %10s  %10s"
        print(t % ("Operation", "Count", "Median", "Total"))
        for label in sorted(self.summary):
            s = self.summary[label]
            print(t % (
                label[:36], s["count"],
                fmt_secs(s["median"]), fmt_secs(s["total"]),
            ))

    def display_comparison(self, previous: dict):
        deltas = compute_deltas(self.summary, previous)

        print()
        print("=" * self.COLS)
        print("  COMPARISON vs previous run")
        print("-" * self.COLS)
        hdr = "  %-36s %8s  %10s  %10s"
        row = "    %-34s %8s  %10s  %10s"
        chg = "    %-34s %8s  %10s  %10s"
        print(hdr % ("Operation", "Count", "Median", "Total"))
        print("=" * self.COLS)

        for label in sorted(self.summary):
            cur = self.summary[label]
            prev = previous.get(label)
            delta = deltas.get(label, {})

            print(f"  {label[:36]}")
            if prev:
                print(row % (
                    "before", prev["count"],
                    fmt_secs(prev["median"]), fmt_secs(prev["total"]),
                ))
            print(row % (
                "after", cur["count"],
                fmt_secs(cur["median"]), fmt_secs(cur["total"]),
            ))
            if prev:
                md = delta.get("median")
                td = delta.get("total")
                md_s = fmt_pct(md) if md is not None and abs(md) > 0.0001 else ""
                td_s = fmt_pct(td) if td is not None and abs(td) > 0.0001 else ""
                if md_s or td_s:
                    print(chg % ("change", "", md_s, td_s))
            print()

        print("=" * self.COLS)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark diskstore get/set/delete/update",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ops", type=int, default=OPS)
    parser.add_argument("--valsize", type=int, default=1000, help="Value size in bytes")
    parser.add_argument("--dbsize", type=int, default=10_000, help="DB item count")
    parser.add_argument("--rounds", type=int, default=3, help="Benchmark rounds")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--results-dir", default=None)
    args = parser.parse_args()

    random.seed(args.seed)

    results_dir = args.results_dir or os.path.join(
        os.path.dirname(__file__) or ".", "bench-results"
    )
    store = ResultsStore(results_dir)

    tmpdir = tempfile.mkdtemp(prefix="diskstore_bench-")
    print(f"Benchmark tmpdir: {tmpdir}")
    print(f"Results dir:      {results_dir}")
    print(f"Ops:              {args.ops}")
    print(f"Value size:       {args.valsize}B")
    print(f"DB size:          {args.dbsize} items")
    print(f"Rounds:           {args.rounds}")

    previous = store.load_last()
    if previous:
        print(f"Previous run:     {previous.get('timestamp', '?')}")
    else:
        print("Previous run:     (first run)")

    bm = Benchmark(
        tmpdir=tmpdir, ops=args.ops, valsize=args.valsize, dbsize=args.dbsize,
        rounds=args.rounds,
    )
    try:
        bm.run_all()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    store.save(
        bm.summary,
        args={"ops": args.ops, "valsize": args.valsize, "dbsize": args.dbsize},
    )

    if previous and previous.get("stats"):
        bm.display_comparison(previous["stats"])
    else:
        bm.display_single()


if __name__ == "__main__":
    main()
