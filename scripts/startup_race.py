#!/usr/bin/env python3
"""Optional load test for simultaneous first opens of a new database.

When several processes open a brand-new database at once, every connection
tries to upgrade its shared lock to an exclusive one for
``PRAGMA journal_mode=WAL``.  SQLite deliberately skips the busy handler when
that upgrade could deadlock and returns ``SQLITE_BUSY`` immediately, even with
a large ``timeout``.  Connection setup retries those transient conditions; this
script reproduces the race and fails if any process still surfaces an error.

This is deliberately *not* part of the pytest suite: it needs ``os.fork``, so
it cannot run on Windows or Pyodide, and it is inherently timing based.  The
deterministic regression tests live in ``tests/test_sqlite_selector.py`` and
``tests/test_diskstore.py``.

Usage:
    uv run scripts/startup_race.py
    uv run scripts/startup_race.py --backend apsw --processes 16 --trials 30
    uv run scripts/startup_race.py --backend sqlite3 --processes 4 --timeout 1
"""

from __future__ import annotations

import argparse
import os
import queue
import shutil
import sys
import tempfile


def worker(path, barrier, results, timeout):
    """Open *path*, synchronise with the other workers, then write a key."""
    try:
        from diskstore import DiskStore  # noqa: PLC0415
        from diskstore.config import BaseConfig  # noqa: PLC0415

        store = DiskStore(path, BaseConfig(timeout=timeout))
        barrier.wait(timeout=30)
        store[os.getpid()] = os.getpid()
        store.close()
        results.put((os.getpid(), None))
    except BaseException as exc:  # classify every outcome
        results.put((os.getpid(), f"{type(exc).__name__}: {exc}"))


def run_trial(mp, path, processes, timeout):
    """Run one simultaneous-open trial and return ``(errors, missing, hung)``."""
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(processes)
    results = ctx.Queue()
    procs = [
        ctx.Process(target=worker, args=(path, barrier, results, timeout))
        for _ in range(processes)
    ]

    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(120)

    outcomes = []
    while True:
        try:
            outcomes.append(results.get(timeout=0.1))
        except queue.Empty:
            break

    hung = [proc for proc in procs if proc.is_alive()]
    for proc in hung:
        proc.terminate()
        proc.join()

    errors = [error for _, error in outcomes if error is not None]
    missing = processes - len(outcomes)
    return errors, missing, len(hung)


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
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="BaseConfig timeout used by every worker",
    )
    args = parser.parse_args()

    if not hasattr(os, "fork"):
        print("SKIP: simultaneous first-open test needs os.fork")
        print("      (not available on Windows or Pyodide)")
        return 0

    try:
        import multiprocessing as mp  # noqa: PLC0415
    except ImportError:
        print("SKIP: multiprocessing is unavailable on this platform")
        return 0

    if args.backend:
        # The backend is selected at import time, so this must precede the
        # import in the workers as well as in this process.
        os.environ["DISKSTORE_BACKEND"] = args.backend

    from diskstore import _sqlite  # noqa: PLC0415

    print(f"backend      = {_sqlite.BACKEND_NAME}")
    print(f"processes    = {args.processes}")
    print(f"trials       = {args.trials}")
    print(f"timeout      = {args.timeout}s")
    print()

    failed_trials = 0
    for trial in range(args.trials):
        workdir = tempfile.mkdtemp(prefix="diskstore-startup-race-")
        path = os.path.join(workdir, "race.db")
        try:
            errors, missing, hung = run_trial(mp, path, args.processes, args.timeout)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

        if errors or missing or hung:
            failed_trials += 1
            print(
                f"trial {trial}: {len(errors)} error(s), "
                f"{missing} missing result(s), {hung} hung process(es)"
            )
            for error in errors[:3]:
                print(f"  {error}")

    print()
    if failed_trials:
        print(
            f"FAIL: {failed_trials}/{args.trials} trials had a failed "
            f"simultaneous first open"
        )
        return 1

    print(f"OK: {args.trials}/{args.trials} simultaneous first opens succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
