#!/usr/bin/env python3
"""A/B benchmark: APSW vs the stdlib ``sqlite3`` driver.

Runs the same workloads against both backends of the *current* checkout by
setting ``DISKSTORE_BACKEND`` in each side's subprocess, then prints a
side-by-side comparison.  Requires the optional ``apsw`` extra (or the dev
group) for the APSW side; without it only the stdlib side runs.

Usage:
    uv run scripts/bench_ab.py
    uv run scripts/bench_ab.py --ops 20000 --rounds 3
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import multiprocessing as mp
import os
import random
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

BATCH = 500
SEED = 42
HAVE_APSW = importlib.util.find_spec("apsw") is not None


def make_value(size: int) -> bytes:
    return random.choice(b"abcdefghijklmnopqrstuvwxyz").to_bytes(1, "little") * size


# ---------------------------------------------------------------------------
# Workloads (run in the per-backend subprocess)
# ---------------------------------------------------------------------------


def _bench(fn, n: int) -> list[float]:
    """Return per-operation timings for *fn* run *n* times in batches."""
    per_op: list[float] = []
    full, rem = divmod(n, BATCH)
    for _ in range(full):
        start = time.perf_counter()
        for _ in range(BATCH):
            fn()
        per_op.append((time.perf_counter() - start) / BATCH)
    if rem:
        start = time.perf_counter()
        for _ in range(rem):
            fn()
        per_op.append((time.perf_counter() - start) / rem)
    return per_op


def run_micro(dbpath: str, ops: int, valsize: int, dbsize: int) -> dict:
    import diskstore  # noqa: PLC0415

    value = make_value(valsize)
    store = diskstore.DiskStore(dbpath)
    store.open()
    keys = [f"k{i}" for i in range(dbsize)]
    store.update(dict.fromkeys(keys, value))

    target = [random.choice(keys) for _ in range(ops)]
    results: dict[str, float] = {}

    counter = {"i": 0}

    def do_set():
        store[target[counter["i"] % len(target)]] = value
        counter["i"] += 1

    results["set"] = statistics.median(_bench(do_set, ops))

    counter["i"] = 0

    def do_get():
        _ = store[target[counter["i"] % len(target)]]
        counter["i"] += 1

    results["get"] = statistics.median(_bench(do_get, ops))

    del_keys = [f"d{i}" for i in range(ops)]
    store.update(dict.fromkeys(del_keys, value))
    counter["i"] = 0

    def do_del():
        try:
            del store[del_keys[counter["i"] % len(del_keys)]]
        except KeyError:
            pass
        counter["i"] += 1

    results["delete"] = statistics.median(_bench(do_del, ops))

    # bulk write inside one explicit transaction
    tx_keys = [f"t{i}" for i in range(ops)]
    start = time.perf_counter()
    with store.transact():
        for key in tx_keys:
            store[key] = value
    results["transact"] = (time.perf_counter() - start) / ops

    # bulk upsert
    bulk = {f"u{i}": value for i in range(ops)}
    start = time.perf_counter()
    store.update(bulk)
    results["update"] = (time.perf_counter() - start) / ops

    store.close()
    return results


def _worker(payload) -> dict:
    num, dbpath, ops, key_range = payload
    import diskstore  # noqa: PLC0415

    random.seed(num)
    time.sleep(0.02)

    store = diskstore.DiskStore(dbpath)
    store.open()
    timings: dict[str, list[float]] = {"get": []}

    for _ in range(ops):
        key = str(random.randrange(key_range))
        start = time.perf_counter()
        try:
            _ = store[key]
        except KeyError:
            pass
        except Exception as exc:
            message = str(exc).lower()
            if "locked" in message or "busy" in message:
                # Contention artifact; retry without recording a timing.
                time.sleep(0.001)
                continue
            raise
        timings["get"].append(time.perf_counter() - start)

    store.close()
    return timings


def run_concurrent(
    dbpath: str, procs: int, ops: int, key_range: int, valsize: int
) -> dict:
    import diskstore  # noqa: PLC0415

    # Seed once from the parent so workers do not race on first-time
    # journal/pragma setup (which would show up as spurious SQLITE_BUSY).
    seed = diskstore.DiskStore(dbpath)
    seed.update({str(i): make_value(valsize) for i in range(key_range)})
    seed.close()

    ctx = mp.get_context("fork")
    payloads = [(num, dbpath, ops, key_range) for num in range(procs)]
    with ctx.Pool(procs) as pool:
        outputs = pool.map(_worker, payloads)

    merged: dict[str, list[float]] = {"get": []}
    for output in outputs:
        for action, values in output.items():
            merged[action].extend(values)
    return {action: statistics.median(vals) for action, vals in merged.items() if vals}


def impl_info() -> dict:
    from diskstore import _sqlite  # noqa: PLC0415

    info: dict = {
        "backend": _sqlite.BACKEND_NAME,
        "python": sys.version.split()[0],
        "apsw": None,
        "sqlite": sqlite3.sqlite_version,
    }
    if _sqlite.BACKEND_NAME == "apsw":
        import apsw  # noqa: PLC0415

        info["apsw"] = apsw.apswversion()
        info["sqlite"] = apsw.sqlitelibversion()
    return info


def _median_of_rounds(rounds: list[dict]) -> dict:
    first = rounds[0]
    return {op: statistics.median([r[op] for r in rounds]) for op in first}


def run_mode(args) -> int:
    sys.path.insert(0, args.src)
    os.makedirs(args.db_dir, exist_ok=True)

    micro_rounds: list[dict] = []
    concurrent_rounds: list[dict] = []
    for round_no in range(args.rounds):
        random.seed(SEED)
        micro_rounds.append(
            run_micro(
                os.path.join(args.db_dir, f"micro-{round_no}.db"),
                args.ops,
                args.valsize,
                args.dbsize,
            )
        )
        concurrent_rounds.append(
            run_concurrent(
                os.path.join(args.db_dir, f"concurrent-{round_no}.db"),
                args.procs,
                args.concurrent_ops,
                args.key_range,
                args.valsize,
            )
        )

    result = {
        "impl": args.name,
        "info": impl_info(),
        "micro": _median_of_rounds(micro_rounds),
        "concurrent": _median_of_rounds(concurrent_rounds),
    }
    with open(args.json_out, "w") as handle:
        json.dump(result, handle, indent=2)
    return 0


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_side(name: str, src: str, out: str, args) -> str:
    env = dict(os.environ, DISKSTORE_BACKEND=name)
    db_dir = tempfile.mkdtemp(prefix=f"diskstore-ab-{name}-")
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "--run",
        "--name",
        name,
        "--src",
        src,
        "--json-out",
        out,
        "--db-dir",
        db_dir,
        "--ops",
        str(args.ops),
        "--valsize",
        str(args.valsize),
        "--dbsize",
        str(args.dbsize),
        "--procs",
        str(args.procs),
        "--concurrent-ops",
        str(args.concurrent_ops),
        "--key-range",
        str(args.key_range),
        "--rounds",
        str(args.rounds),
    ]
    print(f"  running {name} ...", flush=True)
    subprocess.run(cmd, check=True, cwd=repo_root(), env=env)
    return db_dir


def fmt_secs(value: float) -> str:
    if value < 1e-3:
        return f"{value * 1e6:9.2f} us"
    return f"{value * 1e3:9.2f} ms"


def print_report(results: dict, args) -> None:
    names = [name for name in ("apsw", "sqlite3") if name in results]
    print()
    print("=" * 74)
    print("  APSW vs stdlib sqlite3")
    print("=" * 74)
    for name in names:
        info = results[name]["info"]
        version = (
            f"apsw-{info['apsw']}" if name == "apsw" else f"sqlite-{info['sqlite']}"
        )
        print(f"  {name:8s} python={info['python']:8s} {version}")
    print(
        f"  ops={args.ops} valsize={args.valsize} dbsize={args.dbsize} "
        f"procs={args.procs} concurrent_ops={args.concurrent_ops} rounds={args.rounds}"
    )
    print("-" * 74)
    if "apsw" not in results:
        for key, label in (
            ("micro", "micro"),
            ("concurrent", f"concurrent x{args.procs}"),
        ):
            for op in sorted(results["sqlite3"][key]):
                value = results["sqlite3"][key][op]
                print(f"  {label + ' ' + op:22s} {fmt_secs(value):>14s}")
        print("=" * 74)
        print("  apsw not installed: only the stdlib backend was measured.")
        return

    print(f"  {'operation':22s} {'apsw':>14s} {'sqlite3':>14s} {'sqlite3/apsw':>14s}")
    print("-" * 74)
    for key, label in (("micro", "micro"), ("concurrent", f"concurrent x{args.procs}")):
        for op in sorted(results["sqlite3"][key]):
            apsw_val = results["apsw"][key][op]
            sqlite_val = results["sqlite3"][key][op]
            ratio = sqlite_val / apsw_val if apsw_val else float("nan")
            print(
                f"  {label + ' ' + op:22s} {fmt_secs(apsw_val):>14s} "
                f"{fmt_secs(sqlite_val):>14s} {ratio:13.2f}x"
            )
    print("=" * 74)
    print("  'sqlite3/apsw' > 1.0 means APSW is faster for that operation.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="A/B benchmark APSW vs stdlib sqlite3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ops", type=int, default=20_000)
    parser.add_argument("--valsize", type=int, default=1000)
    parser.add_argument("--dbsize", type=int, default=10_000)
    parser.add_argument("--procs", type=int, default=4)
    parser.add_argument("--concurrent-ops", type=int, default=5_000)
    parser.add_argument("--key-range", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--results-dir", default=None)
    # internal: run one side in this process and write JSON
    parser.add_argument("--run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--name", default="", help=argparse.SUPPRESS)
    parser.add_argument("--src", default="", help=argparse.SUPPRESS)
    parser.add_argument("--json-out", default="", help=argparse.SUPPRESS)
    parser.add_argument("--db-dir", default="", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.run:
        return run_mode(args)

    root = repo_root()
    src = os.path.join(root, "src")
    results_dir = args.results_dir or os.path.join(root, "scripts", "bench-results")
    os.makedirs(results_dir, exist_ok=True)

    results: dict = {}
    for name in ("apsw", "sqlite3"):
        if name == "apsw" and not HAVE_APSW:
            print("apsw not installed; skipping the APSW side")
            continue
        out = os.path.join(results_dir, f"ab-{name}.json")
        db_dir = None
        try:
            db_dir = run_side(name, src, out, args)
            with open(out) as handle:
                results[name] = json.load(handle)
        finally:
            if db_dir is not None:
                shutil.rmtree(db_dir, ignore_errors=True)
    print_report(results, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
