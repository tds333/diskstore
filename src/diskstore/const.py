"""Core disktore API."""

from typing import (
    TypeAlias,
    Union,
)

TIMEOUT = 60.0  # in seconds

MISSING = object()


DEFAULT_PRAGMAS = {
    "auto_vacuum": 0,  # 1=FULL, 0=None
    "cache_size": 2**13,  # 8,192 pages
    "journal_mode": "wal",
    "mmap_size": 2**28,  # 256 MB
    "synchronous": 1,  # 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA
    "temp_store": "memory",
    # "pargma_busy_timeout": 5000,  # milliseconds
}


AnyLite: TypeAlias = Union[bytes, str, int, float]
KeyType = AnyLite
