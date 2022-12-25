from __future__ import generator_stop

import logging
from collections.abc import Mapping, MutableMapping
import gzip
import zstandard as zstd
from sys import exit
from typing import Any, Generic, Iterator, List, Optional, Tuple, TypeVar, Union
import lmdb
import pickle

from . import utils

# logger = logging.getLogger(__name__)

#######################################################
### Classes


class Shock(MutableMapping):

    def __init__(self, file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = True, mode: int = 0o755):
        """

        """
        if flag == "r":  # Open existing database for reading only (default)
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=True, create=False, mode=mode, subdir=False, lock=lock)
            write = False
        elif flag == "w":  # Open existing database for reading and writing
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=False, mode=mode, subdir=False, lock=lock)
            write = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, mode=mode, subdir=False, lock=lock)
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            utils.remove_db(file_path)
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, mode=mode, subdir=False, lock=lock)
            write = True
        else:
            raise ValueError("Invalid flag")

        self.txn = env.begin(write=write, buffers=False)
        self.env = env
        self._write = write

    @property
    def map_size(self) -> int:

        return self.env.info()["map_size"]

    @map_size.setter
    def map_size(self, value: int) -> None:

        self.env.set_mapsize(value)

    def _pre_key(self, key: str) -> bytes:

        return key.encode()


    def _post_key(self, key: bytes) -> str:

        return key.decode()

    def _pre_value(self, value) -> bytes:

        if isinstance(value, bytes):
            return value
        else:
            return pickle.dumps(value)

    def _post_value(self, value: bytes):

        return pickle.loads(value)

    def __getitem__(self, key: str):

        value = self.txn.get(self._pre_key(key))
        if value is None:
            raise KeyError(key)
        return self._post_value(value)

    def __setitem__(self, key: str, value) -> None:

        k = self._pre_key(key)
        v = self._pre_value(value)
        self.txn.put(k, v)

    def __delitem__(self, key: str) -> None:

        self.txn.delete(self._pre_key(key))

    def keys(self):

        # return self.txn.cursor().iternext(keys=True, values=False)

        for key in self.txn.cursor().iternext(keys=True, values=False):
            yield self._post_key(key)

    def items(self):

        # return self.txn.cursor().iternext(keys=True, values=True)

        for key, value in self.txn.cursor().iternext(keys=True, values=True):
            yield (self._post_key(key), self._post_value(value))

    def values(self):

        # return self.txn.cursor().iternext(keys=False, values=True)

        for value in self.txn.cursor().iternext(keys=False, values=True):
            yield self._post_value(value)

    def __contains__(self, key: str) -> bool:

        return self.txn.cursor().set_key(self._pre_key(key))

    def __iter__(self):

        return self.keys()

    def __len__(self) -> int:

        return self.txn.stat()["entries"]

    def pop(self, key: str, default=None):

        value = self.txn.pop(self._pre_key(key))

        if value is None:
            if default is None:
                raise KeyError(key)
            else:
                return default

        return self._post_value(value)

    # def update(self, __other: Any = (), **kwds: VT) -> None:  # python3.8 only: update(self, other=(), /, **kwds)

    #     # fixme: `kwds`

    #     # note: benchmarking showed that there is no real difference between using lists or iterables
    #     # as input to `putmulti`.
    #     # lists: Finished 14412594 in 253496 seconds.
    #     # iter:  Finished 14412594 in 256315 seconds.

    #     # save generated lists in case the insert fails and needs to be retried
    #     # for performance reasons, but mostly because `__other` could be an iterable
    #     # which would already be exhausted on the second try
    #     pairs_other: Optional[List[Tuple[bytes, bytes]]] = None
    #     pairs_kwds: Optional[List[Tuple[bytes, bytes]]] = None

    #     for i in range(12):
    #         try:
    #             with self.env.begin(write=True) as txn:
    #                 with txn.cursor() as curs:
    #                     if isinstance(__other, Mapping):
    #                         pairs_other = pairs_other or [
    #                             (self._pre_key(key), self._pre_value(__other[key])) for key in __other
    #                         ]
    #                         curs.putmulti(pairs_other)
    #                     elif hasattr(__other, "keys"):
    #                         pairs_other = pairs_other or [
    #                             (self._pre_key(key), self._pre_value(__other[key])) for key in __other.keys()
    #                         ]
    #                         curs.putmulti(pairs_other)
    #                     else:
    #                         pairs_other = pairs_other or [
    #                             (self._pre_key(key), self._pre_value(value)) for key, value in __other
    #                         ]
    #                         curs.putmulti(pairs_other)

    #                     pairs_kwds = pairs_kwds or [
    #                         (self._pre_key(key), self._pre_value(value)) for key, value in kwds.items()
    #                     ]
    #                     curs.putmulti(pairs_kwds)

    #                     return
    #         except lmdb.MapFullError:
    #             if not self.autogrow:
    #                 raise
    #             new_map_size = self.map_size * 2
    #             self.map_size = new_map_size
    #             logger.info(self.autogrow_msg, self.env.path(), new_map_size)

    #     exit(self.autogrow_error.format(self.env.path()))

    def sync(self) -> None:

        if self._write:
            self.txn.commit()
            self.env.sync()
            self.txn = self.env.begin(write=self._write, buffers=False)

    def close(self) -> None:

        self.sync()
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ShockGzip(Shock):
    def __init__(self, file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = True, mode: int = 0o755, compresslevel: int = 9):
        Shock.__init__(self, file_path, flag, map_size, lock, mode)

        self.compresslevel = compresslevel

    def _pre_value(self, value) -> bytes:

        value = Shock._pre_value(self, value)

        return gzip.compress(value, self.compresslevel)

    def _post_value(self, value: bytes):

        return gzip.decompress(value)


def open(
    file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = True, mode: int = 0o755):

    """
    Opens the database `file`.
    `flag`: r (read only, existing), w (read and write, existing),
            c (read, write, create if not exists), n (read, write, overwrite existing)
    `map_size`: Initial database size. Defaults to 2**40 (1TB).
    """

    return Shock(file_path, flag, map_size, lock, mode)



# for key, value in shock0.items():
#     print(key)




