# ruff: noqa: E501
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "diskcache>=5.6.3",
#     "diskstore[apsw]",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmark DiskStore (both SQLite backends) against diskcache.Index.

Each configuration runs in its own subprocess so the SQLite backend can be
selected per run through the ``DISKSTORE_BACKEND`` environment variable
(``apsw`` or ``sqlite3``), giving a like-for-like comparison:

    uv run scripts/benchmark_core.py -p 1
    uv run scripts/benchmark_core.py -p 8
"""

import argparse
import collections as co
import json
import multiprocessing as mp
import os
import pickle
import random
import shutil
import subprocess
import sys
import tempfile
import time

from utils import display, percentile, secs

PROCS = 8
OPS = int(1e5)
RANGE = 100
WARMUP = int(1e3)
SIZE = 1024

example_data = json.dumps(
    json.loads("""
{
"productId": 1001,
"productName": "Wireless Headphones",
"description": "Noise-cancelling wireless headphones with Bluetooth 5.0 and 20-hour battery life.",
"brand": "SoundPro",
"category": "Electronics",
"price": 199.99,
"currency": "USD",
"stock": {
    "available": true,
    "quantity": 50
},
"images": [
    "https://example.com/products/1001/main.jpg",
    "https://example.com/products/1001/side.jpg"
],
"variants": [
    {
    "variantId": "1001_01",
    "color": "Black",
    "price": 199.99,
    "stockQuantity": 20
    },
    {
    "variantId": "1001_02",
    "color": "White",
    "price": 199.99,
    "stockQuantity": 30
    }
],
"dimensions": {
    "weight": "0.5kg",
    "width": "18cm",
    "height": "20cm",
    "depth": "8cm"
},
"ratings": {
    "averageRating": 4.7,
    "numberOfReviews": 120
},
"reviews": [
    {
    "reviewId": 501,
    "userId": 101,
    "username": "techguy123",
    "rating": 5,
    "comment": "Amazing sound quality and battery life!"
    },
    {
    "reviewId": 502,
    "userId": 102,
    "username": "jane_doe",
    "rating": 4,
    "comment": "Great headphones but a bit pricey."
    }
]
}
""")
)

# (kind, DISKSTORE_BACKEND, label)
CONFIGS = (
    ("diskstore", "apsw", "diskstore.DiskStore (apsw)"),
    ("diskstore", "sqlite3", "diskstore.DiskStore (sqlite3)"),
    ("diskcache", None, "diskcache.Index"),
)


def worker(num, kind, args, kwargs):
    random.seed(num)

    time.sleep(0.01)  # Let other processes start.

    obj = kind(*args, **kwargs)
    if hasattr(obj, "open"):
        obj.open()

    timings = co.defaultdict(list)
    value = example_data

    for count in range(OPS):
        key = str(random.randrange(RANGE)).encode("utf-8")
        # value = str(count).encode("utf-8") * random.randrange(1, 100)
        choice = random.random()

        if choice < 0.900:
            start = time.time()
            result = None
            try:
                result = obj[key]
            except KeyError:
                pass
            end = time.time()
            miss = result is None
            action = "get"
        elif choice < 0.990:
            start = time.time()
            result = obj[key] = value
            end = time.time()
            miss = result is False
            action = "set"
        else:
            start = time.time()
            miss = False
            try:
                del obj[key]
            except KeyError:
                miss = True
            end = time.time()
            action = "delete"

        if count > WARMUP:
            delta = end - start
            timings[action].append(delta)
            if miss:
                timings[action + "-miss"].append(delta)

    with open("output-%d.pkl" % num, "wb") as writer:
        pickle.dump(timings, writer, protocol=pickle.HIGHEST_PROTOCOL)


def _cache(kind):
    if kind == "diskcache":
        import diskcache  # noqa: PLC0415

        return diskcache.Index
    import diskstore  # noqa: PLC0415

    return diskstore.DiskStore


def run_config(  # noqa: PLR0913, PLR0917
    kind, tmpdir, out, procs, ops, key_range, warmup
):
    """Run a single configuration's workers and write its timings to *out*."""
    global OPS, PROCS, RANGE, WARMUP  # noqa: PLW0603
    OPS = ops
    PROCS = procs
    RANGE = key_range
    WARMUP = warmup

    cache_kind = _cache(kind)
    path = os.path.join(tmpdir, "cache") if kind == "diskstore" else tmpdir

    obj = cache_kind(path)
    for index in range(RANGE):
        key = str(index).encode("utf-8")
        obj[key] = key
    try:
        obj.close()
    except Exception:
        pass

    processes = [
        mp.Process(target=worker, args=(num, cache_kind, (path,), {}))
        for num in range(PROCS)
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    timings = co.defaultdict(list)

    for num in range(PROCS):
        filename = "output-%d.pkl" % num

        with open(filename, "rb") as reader:
            output = pickle.load(reader)

        for key in output:
            timings[key].extend(output[key])

        os.remove(filename)

    payload = {key: list(value) for key, value in timings.items()}
    for action in ("get", "set", "delete"):
        payload.setdefault(action, [])

    with open(out, "w") as handle:
        json.dump(payload, handle)


def compare(results):
    actions = ("get", "set", "delete")
    name_width = max(len(name) for name, _ in results)
    header = "  %-*s %11s %11s %11s" % (name_width, "median/op", *actions)
    print()
    print("=" * len(header))
    print("  Summary (median per operation)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for name, timings in results:
        cells = " ".join(
            "%11s" % secs(percentile(timings.get(action, []), 0.5))
            for action in actions
        )
        print("  %-*s %s" % (name_width, name, cells))

    print("=" * len(header))


def dispatch(procs, ops, key_range, warmup):
    tmp_directory = tempfile.mkdtemp(prefix="diskstore_bench-")
    results = []

    try:
        for index, (kind, backend, label) in enumerate(CONFIGS):
            env = dict(os.environ)
            if backend:
                env["DISKSTORE_BACKEND"] = backend
            else:
                env.pop("DISKSTORE_BACKEND", None)

            subdir = os.path.join(tmp_directory, "cfg%d" % index)
            os.makedirs(subdir)
            out = os.path.join(tmp_directory, "result%d.json" % index)

            cmd = [
                sys.executable,
                os.path.abspath(__file__),
                "--run-config",
                kind,
                "--tmpdir",
                subdir,
                "--out",
                out,
                "--procs",
                str(procs),
                "--ops",
                str(ops),
                "--key-range",
                str(key_range),
                "--warmup",
                str(warmup),
            ]

            print()
            print("Running %s ..." % label)
            try:
                subprocess.run(cmd, check=True, env=env)
            except subprocess.CalledProcessError:
                print("Skipping %s (failed to run)" % label)
                continue

            with open(out) as handle:
                timings = json.load(handle)

            display(label, timings)
            results.append((label, timings))
    finally:
        shutil.rmtree(tmp_directory, ignore_errors=True)

    if results:
        compare(results)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-p",
        "--processes",
        type=int,
        default=PROCS,
        help="Number of processes to start",
    )
    parser.add_argument(
        "-n",
        "--operations",
        type=float,
        default=OPS,
        help="Number of operations to perform",
    )
    parser.add_argument(
        "-r",
        "--range",
        type=int,
        default=RANGE,
        help="Range of keys",
    )
    parser.add_argument(
        "-w",
        "--warmup",
        type=float,
        default=WARMUP,
        help="Number of warmup operations before timings",
    )
    # internal: run one configuration in this subprocess
    parser.add_argument(
        "--run-config", choices=("diskstore", "diskcache"), default=None
    )
    parser.add_argument("--tmpdir", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--procs", type=int, default=PROCS)
    parser.add_argument("--ops", type=int, default=OPS)
    parser.add_argument("--key-range", type=int, default=RANGE)
    args = parser.parse_args()

    if args.run_config:
        run_config(
            args.run_config,
            args.tmpdir,
            args.out,
            args.procs,
            args.ops,
            args.key_range,
            int(args.warmup),
        )
        return

    print("len example_data:", len(example_data))
    dispatch(int(args.processes), int(args.operations), args.range, int(args.warmup))


if __name__ == "__main__":
    main()
