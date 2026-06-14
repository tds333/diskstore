"""Default constants used in diskstore."""

from typing import (
    TypeAlias,
    Union,
)

TIMEOUT = 10.0
"default timout in seconds"

MISSING = object()

DEFAULT_RO_PRAGMAS = {
    "cache_size": 2**13,  # 8,192 pages
    "mmap_size": 2**28,  # 256 MB
    "temp_store": 2,  # 0=DEFAULT, 1=FILE, 2=MEMORY
    "synchronous": 1,  # 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA
}
"default read only pragma settings"

DEFAULT_PRAGMAS = {
    "auto_vacuum": 0,  # 1=FULL, 0=None
    "cache_size": 2**13,  # 8,192 pages
    "journal_mode": "wal",
    "mmap_size": 2**28,  # 256 MB
    "synchronous": 1,  # 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA
    "temp_store": 2,  # 0=DEFAULT, 1=FILE, 2=MEMORY
    # "pargma_busy_timeout": 5000,  # milliseconds
}
"default pragma settings"


AnyLite: TypeAlias = Union[bytes, str, int, float]
KeyType = AnyLite
