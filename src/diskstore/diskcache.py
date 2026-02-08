"""Core disktore API."""

import os
import os.path
import pickle
from time import time
from typing import (
    NamedTuple,
)

from .const import MISSING, TIMEOUT, KeyType
from .diskstore import DiskStore


class CacheValue(NamedTuple):
    value: bytes
    expires_at: float

    def load(self, expire_timestamp, default=MISSING):
        expires_at = self[1]
        if expires_at == 0.0 or expires_at > time():
            return pickle.loads(self[0])
        return default

    @classmethod
    def create(cls, value, expires=None):
        expires_at = 0.0
        if expires:
            expires_at = time() + expires
        return cls(value=pickle.dumps(value), expires_at=expires_at)


class DiskCache:
    def __init__(
        self,
        filename: os.PathLike | str,
        default_expiration=60.0,
    ) -> None:
        """Initialize DiskCache instance.

        :param str filename: DiskStore DB filename.
                              If not set a tmp directory and default DB name is created.
        :param float default_expiration: Default expiration value in seconds (float)
        """
        self._diskstore = DiskStore(
            filename=filename,
            value_class=CacheValue,
            tablename="Cache",
            timeout=TIMEOUT,
            pragma_auto_vacuum=1,
            pragma_synchronous=0,
            alter_table=False,
        )
        self._default_expiration = default_expiration

    def delete(self, key: KeyType) -> bool:
        bin_key = pickle.dumps(key)
        try:
            del self._diskstore[bin_key]
            return True
        except KeyError:
            return False

    def set(self, key, value, expire=None):
        bin_key = pickle.dumps(key)
        if expire is None:
            expire = self._default_expiration
        v = CacheValue.create(value, expire)
        self._diskstore[bin_key] = v

    def get(
        self,
        key: KeyType,
        default=None,
    ):
        bin_key = pickle.dumps(key)
        try:
            result = self._diskstore[bin_key]
        except KeyError:
            return default

        return result.load(expire_timestamp=time(), default=default)

    def add(self, key, value, expire=None):
        bin_key = pickle.dumps(key)
        v = CacheValue.create(value, expire)
        ret_key = self._diskstore.add(bin_key, v)
        if ret_key is None:
            return False

        return True

    def delete_expired(self):
        delete_expired = "DELETE FROM Cache WHERE expires_at != 0.0 AND expires_at < ?"
        with self._diskstore.transact() as cx:
            cx.execute(delete_expired, (time(),))

    def touch(self, key, expire=None):
        bin_key = pickle.dumps(key)
        if expire is None:
            expire = self._default_expiration
        expires_at = time() + expire if expire else 0.0
        stmt = "UPDATE Cache SET expires_at = ? WHERE _key = ?"
        with self._diskstore.transact() as cx:
            cx.execute(stmt, (expires_at, bin_key))

    @property
    def filename(self):
        return self._diskstore.filename

    def check(self, *args, **kwargs):
        return self._diskstore.check(*args, **kwargs)

    def clear(self):
        self._diskstore.clear()

    def close(self):
        self._diskstore.close()

    def __enter__(self):
        self._diskstore.__enter__()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._diskstore.__exit__(*args, **kwargs)

    def __len__(self):
        return len(self._diskstore)

    def __iter__(self):
        for key in self._diskstore:
            yield pickle.loads(key)

    def __reversed__(self):
        for key in reversed(self._diskstore):
            yield pickle.loads(key)
