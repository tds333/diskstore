"""Stress test DiskStore."""

import itertools as it
import multiprocessing as mp
import random
import time

from diskstore import DiskStore, Value

KEYS = 10_000
OPERATIONS = 100_000
SEED = 0

functions = []


def register(function):
    functions.append(function)
    return function


@register
def stress_get(store):
    key = random.randrange(KEYS)
    store.get(key, None)


@register
def stress_set(store):
    key = random.randrange(KEYS)
    value = random.random()
    store[key] = Value(value)


register(stress_set)
register(stress_set)
register(stress_set)


@register
def stress_del(store):
    key = random.randrange(KEYS)

    try:
        del store[key]
    except KeyError:
        pass


@register
def stress_pop(store):
    key = random.randrange(KEYS)
    store.pop(key, None)


@register
def stress_popitem(store):
    try:
        if random.randrange(2):
            store.popitem()
    except KeyError:
        pass


@register
def stress_iter(store):
    iterator = it.islice(store, 5)

    for key in iterator:
        pass


@register
def stress_reversed(store):
    iterator = it.islice(reversed(store), 5)

    for key in iterator:
        pass


@register
def stress_len(store):
    len(store)


def stress(seed, store):
    random.seed(seed)
    for count in range(OPERATIONS):
        function = random.choice(functions)
        function(store)


def test(status=False):
    random.seed(SEED)
    # store = DiskStore("/tmp/diskstore_stress_store_mp.db")
    store = DiskStore("/tmp/diskstore_stress_store_mp.db", timeout=5)
    store.update((key, Value(value)) for key, value in enumerate(range(KEYS)))
    processes = []

    for count in range(8):
        process = mp.Process(target=stress, args=(SEED + count, store))
        process.start()
        processes.append(process)

    for value in it.count():
        time.sleep(1)

        if status:
            print("\r", value, "s", len(store), "keys", " " * 20, end="")

        if all(not process.is_alive() for process in processes):
            break

    if status:
        print("")

    assert all(process.exitcode == 0 for process in processes)


if __name__ == "__main__":
    test(status=True)
