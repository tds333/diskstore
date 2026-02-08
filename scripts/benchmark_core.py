# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "diskcache>=5.6.3",
#     "diskstore",
# ]
#
# [tool.uv.sources]
# diskstore = { path = "../", editable = true }
# ///
"""Benchmark diskcache.Cache

$ export PYTHONPATH=/Users/grantj/repos/python-diskcache
$ python tests/benchmark_core.py -p 1 > tests/timings_core_p1.txt
$ python tests/benchmark_core.py -p 8 > tests/timings_core_p8.txt
"""

import collections as co

# from codecs import ignore_errors
import json
import multiprocessing as mp
import os
import pickle
import random
import shutil
import tempfile
import time

from utils import display

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

caches = []

###############################################################################
# DiskStore Benchmarks
###############################################################################

import diskcache

import diskstore


def worker(num, kind, args, kwargs):
    random.seed(num)

    time.sleep(0.01)  # Let other processes start.

    obj = kind(*args, **kwargs)

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
            result = obj[key] = diskstore.Value(value)
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


def dispatch():
    # tmp_directory = "/data/tmp/bench"
    # os.makedirs(tmp_directory)
    tmp_directory = tempfile.mkdtemp(prefix="diskstore_bench-")

    caches.append(
        (
            "diskstore.DiskStore",
            diskstore.DiskStore,
            (os.path.join(tmp_directory, "diskstore.db"),),
            # (":memory:",),
            {},
        )
    )

    # import diskcache  # noqa

    caches.append(
        (
            "diskcache.Index",
            diskcache.Index,
            (tmp_directory,),
            {},
        )
    )

    for name, kind, args, kwargs in caches:
        obj = kind(*args, **kwargs)

        for key in range(RANGE):
            key = str(key).encode("utf-8")
            obj[key] = diskstore.Value(key)

        try:
            obj.close()
        except Exception:
            pass

        processes = [
            mp.Process(target=worker, args=(value, kind, args, kwargs))
            for value in range(PROCS)
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

        display(name, timings)
    shutil.rmtree(tmp_directory, ignore_errors=True)


if __name__ == "__main__":
    import argparse

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

    args = parser.parse_args()

    PROCS = int(args.processes)
    OPS = int(args.operations)
    RANGE = int(args.range)
    WARMUP = int(args.warmup)
    print("len example_data:", len(example_data))
    dispatch()
