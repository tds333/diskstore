"""Stress test diskcache.persistent.Index."""

import collections as co
import itertools as it
import os
import random
from contextlib import suppress

import diskstore as ds

KEYS = 1000
OPERATIONS = 250000
SEED = 0

functions = []


def register(function):
    functions.append(function)
    return function


@register
def stress_get(mapping, index):
    key = random.randrange(KEYS)
    assert mapping.get(key, None) == index.get(key, None)


@register
def stress_getitem(mapping, index):
    key = random.randrange(KEYS)
    result = True
    try:
        result = mapping[key] == index[key]
    except KeyError:
        pass
    assert result


@register
def stress_set(mapping, index):
    key = random.randrange(KEYS)
    value = random.random()
    mapping[key] = ds.Value(value)
    index[key] = ds.Value(value)


register(stress_set)
register(stress_set)
register(stress_set)


@register
def stress_pop(mapping, index):
    key = random.randrange(KEYS)
    assert mapping.pop(key, None) == index.pop(key, None)


@register
def stress_popitem(mapping, index):
    if len(mapping) == len(index) == 0:
        return
    elif random.randrange(2):
        assert mapping.popitem() == index.popitem()


@register
def stress_iter(mapping, index):
    iterator = it.islice(zip(mapping, index), 5)
    assert all(alpha == beta for alpha, beta in iterator)


@register
def stress_reversed(mapping, index):
    reversed_mapping = reversed(mapping)
    reversed_index = reversed(index)
    pairs = it.islice(zip(reversed_mapping, reversed_index), 5)
    assert all(alpha == beta for alpha, beta in pairs)


@register
def stress_len(mapping, index):
    assert len(mapping) == len(index)


def stress(mapping, index):
    for count in range(OPERATIONS):
        function = random.choice(functions)
        function(mapping, index)

        if count % 1000 == 0:
            print("\r", len(mapping), " " * 7, end="")

    print()


def test():
    random.seed(SEED)
    mapping = co.OrderedDict(
        (key, ds.Value(value)) for key, value in enumerate(range(KEYS))
    )
    filename = "/tmp/diskstore_stress_test_index.db"
    with suppress(FileNotFoundError):
        os.remove(filename)
    index = ds.DiskStore(filename)
    index.update(mapping)
    assert mapping == index
    stress(mapping, index)
    assert mapping == index


if __name__ == "__main__":
    test()
