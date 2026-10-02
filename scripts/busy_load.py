#!/usr/bin/env python3
"""Load-test diskstore's busy-error handling against one SQLite driver.

Runs many processes hammering the same database and asserts the invariant
that matters: *every* busy condition must surface as diskstore's own
``BusyError``, never as a raw driver error.  Three separate call sites used
to leak raw errors under exactly this kind of load (``clear()``,
``check(vacuum=True)`` and ``set_pragma()``), each of which bypassed the
translating ``execute()`` wrapper.

Deliberately no PEP 723 inline metadata, so it runs in the project
environment the way ``scripts/bench_ab.py`` does.

Usage:
    uv run scripts/busy_load.py
    uv run scripts/busy_load.py --backend apsw --processes 8 --ops 800
    uv run scripts/busy_load.py --backend sqlite3 --processes 4 --ops 400

Exits non-zero if a busy condition escaped as anything other than
``BusyError``, so it can be used as a gate.
"""

from __future__ import annotations

import argparse
import collections
import os
import random
import sys
import tempfile
import time
from functools import partial

BUSY_CODES = {
    5: "SQLITE_BUSY",
    261: "SQLITE_BUSY_RECOVERY",
    517: "SQLITE_BUSY_SNAPSHOT",
    773: "SQLITE_BUSY_TIMEOUT",
}


def worker(payload):
    """Hammer the database from one process, classifying every outcome.

    Returns ``(counts, leaks, codes)`` where *leaks* holds any busy condition
    that escaped as something other than ``diskstore.BusyError``.
    """
    # The backend is selected at import time, so this must precede the import.
    path, ops, seed, value_size = payload

    from diskstore import DiskStore  # noqa: PLC0415
    from diskstore._sqlite import BusyError  # noqa: PLC0415
    from diskstore.config import BaseConfig  # noqa: PLC0415

    random.seed(seed)
    # A short timeout keeps the run quick while still generating contention;
    # a real deployment would use the default 10s.
    store = DiskStore(path, BaseConfig(timeout=0.05))

    counts: collections.Counter = collections.Counter()
    leaks: collections.Counter = collections.Counter()
    codes: collections.Counter = collections.Counter()

    def attempt(label, fn):
        try:
            fn()
        except KeyError:
            # A missing key is a legitimate race outcome for get/del/pop.
            counts[f"{label}:KeyError"] += 1
        except BusyError as exc:
            counts[f"{label}:BusyError"] += 1
            code = getattr(exc, "extendedresult", None) or getattr(
                exc, "sqlite_errorcode", None
            )
            if code is not None:
                codes[BUSY_CODES.get(code, str(code))] += 1
        except BaseException as exc:  # classifying every outcome is the point
            name = f"{type(exc).__module__}.{type(exc).__name__}"
            counts[f"{label}:{name}"] += 1
            code = getattr(exc, "extendedresult", None) or getattr(
                exc, "sqlite_errorcode", None
            )
            is_busy_code = code in BUSY_CODES
            if is_busy_code or "busy" in type(exc).__name__.lower():
                # The bug this script exists to catch.
                leaks[f"{label}: {name} code={code} {exc}"] += 1
        else:
            counts[label] += 1

    keys = 200
    for i in range(ops):
        key = (i * 7919) % keys
        value = b"v" * value_size
        attempt("get", lambda k=key: store.get(k))
        attempt("set", lambda k=key, v=value: store.__setitem__(k, v))
        attempt("del", lambda k=key: store.__delitem__(k))
        attempt("update", lambda k=key, v=value: store.update({k: v}))
        attempt("popitem", store.popitem)

        def in_transact(k=key, v=value):
            with store.transact():
                store[k] = v

        attempt("transact", in_transact)

        if random.random() < 0.02:
            attempt("clear", store.clear)
            attempt("check_vacuum", partial(store.check, vacuum=True))

    store.close()
    return counts, leaks, codes


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        choices=("apsw", "sqlite3"),
        help="driver to test (default: whichever diskstore auto-selects)",
    )
    parser.add_argument("--processes", type=int, default=8)
    parser.add_argument("--ops", type=int, default=400, help="ops per process")
    parser.add_argument("--value-size", type=int, default=64)
    args = parser.parse_args()

    if args.backend:
        os.environ["DISKSTORE_BACKEND"] = args.backend

    import multiprocessing as mp  # noqa: PLC0415

    from diskstore import DiskStore, _sqlite  # noqa: PLC0415
    from diskstore.config import BaseConfig  # noqa: PLC0415

    # Decide before forking so every worker inherits the same backend.
    backend = args.backend or _sqlite.BACKEND_NAME
    print(f"backend      = {backend}")
    print(f"processes    = {args.processes}")
    print(f"ops/process  = {args.ops}")
    print(f"BusyError    = {_sqlite.BusyError.__module__}.{_sqlite.BusyError.__name__}")
    print(f"SQLError     = {_sqlite.SQLError.__module__}.{_sqlite.SQLError.__name__}")
    print()

    workdir = tempfile.mkdtemp(prefix="diskstore-busy-load-")
    path = os.path.join(workdir, "load.db")
    DiskStore(path, BaseConfig()).close()

    ctx = mp.get_context("fork")
    payloads = [
        (path, args.ops, seed, args.value_size) for seed in range(args.processes)
    ]

    start = time.perf_counter()
    with ctx.Pool(args.processes) as pool:
        results = pool.map(worker, payloads)
    elapsed = time.perf_counter() - start

    counts: collections.Counter = collections.Counter()
    leaks: collections.Counter = collections.Counter()
    codes: collections.Counter = collections.Counter()
    for c, leak, code in results:
        counts.update(c)
        leaks.update(leak)
        codes.update(code)

    total_ops = sum(v for k, v in counts.items() if ":" not in k)
    print(f"elapsed      = {elapsed:.2f}s")
    print(f"total ops    = {total_ops}")
    print()

    print("outcomes (classification only - counts vary run to run):")
    for name in sorted(counts):
        print(f"  {name:44} {counts[name]}")
    print()

    if codes:
        print("busy codes seen:")
        for name in sorted(codes):
            print(f"  {name:44} {codes[name]}")
        print()

    if leaks:
        print(f"FAIL: {sum(leaks.values())} busy condition(s) escaped as a raw error:")
        for name in sorted(leaks):
            print(f"  {name}  x{leaks[name]}")
        return 1

    print("OK: every busy condition surfaced as diskstore.BusyError")
    return 0


if __name__ == "__main__":
    sys.exit(main())
