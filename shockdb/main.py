from __future__ import generator_stop

import logging
from collections.abc import Mapping, MutableMapping
import gzip
import zstandard as zstd
from sys import exit
from typing import Any, Generic, Iterator, List, Optional, Tuple, TypeVar, Union
import lmdb
import pickle

# from . import utils
import utils

# logger = logging.getLogger(__name__)

#######################################################
### Classes


class Shock(MutableMapping):

    def __init__(self, file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = False, sync: bool = False, max_readers: int = 126):
        """

        """
        if flag == "r":  # Open existing database for reading only (default)
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=True, create=False, subdir=False, lock=lock, sync=False, max_readers=max_readers)
            write = False
        elif flag == "w":  # Open existing database for reading and writing
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=False, subdir=False, lock=lock, sync=sync, max_readers=max_readers)
            write = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, subdir=False, lock=lock, sync=sync, max_readers=max_readers)
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            utils.remove_db(file_path)
            env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, subdir=False, lock=lock, sync=sync, max_readers=max_readers)
            write = True
        else:
            raise ValueError("Invalid flag")

        self.env = env
        self._write = write

    def info(self) -> dict:

        return self.env.info()

    def set_map_size(self, value: int) -> None:

        self.env.set_mapsize(value)

    def copy(self, file_path, compact=True):
        """
        Make a consistent copy of the environment in the given destination file path.

        Parameters
        ----------
        file_path : str or path-like
            Path to new file.
        compact:
            If True, perform compaction while copying: omit free pages and sequentially renumber all pages in output. This option consumes more CPU and runs more slowly than the default, but may produce a smaller output database.
        """
        self.env.copy(file_path, compact=compact)


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

        with self.env.begin(write=False, buffers=False) as txn:
            value = txn.get(self._pre_key(key))

        if value is None:
            raise KeyError(key)
        return self._post_value(value)

    def __setitem__(self, key: str, value) -> None:
        if self._write:
            with self.env.begin(write=True, buffers=False) as txn:
                k = self._pre_key(key)
                v = self._pre_value(value)
                txn.put(k, v)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key: str) -> None:
        if self._write:
            with self.env.begin(write=True, buffers=False) as txn:
                txn.delete(self._pre_key(key))
        else:
            raise ValueError('File is open for read only.')

    def keys(self):

        with self.env.begin(write=False, buffers=False) as txn:
            for key in txn.cursor().iternext(keys=True, values=False):
                yield self._post_key(key)

    def items(self):

        with self.env.begin(write=False, buffers=False) as txn:
            for key, value in txn.cursor().iternext(keys=True, values=True):
                yield (self._post_key(key), self._post_value(value))

    def values(self):

        with self.env.begin(write=False, buffers=False) as txn:
            for value in txn.cursor().iternext(keys=False, values=True):
                yield self._post_value(value)

    def __contains__(self, key: str) -> bool:

        with self.env.begin(write=False, buffers=False) as txn:
            return txn.cursor().set_key(self._pre_key(key))

    def __iter__(self):

        return self.keys()

    def __len__(self) -> int:

        with self.env.begin(write=False, buffers=False) as txn:
            return txn.stat()["entries"]

    def pop(self, key: str, default=None):

        if self._write:
            with self.env.begin(write=True, buffers=False) as txn:
                value = txn.pop(self._pre_key(key))
        else:
            raise ValueError('File is open for read only.')

        if value is None:
            if default is None:
                raise KeyError(key)
            else:
                return default

        return self._post_value(value)

    # def clear(self, delete=False):
    #     if self._write:
    #         with self.env.begin(write=True, buffers=False) as txn:
    #             txn.drop(db=None, delete=delete)
    #     else:
    #         raise ValueError('File is open for read only.')


    def sync(self) -> None:

        if self._write:
            self.env.sync()

    def close(self) -> None:

        self.sync()
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ShockGzip(Shock):
    def __init__(self, file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = True, compresslevel: int = 9):
        Shock.__init__(self, file_path, flag, map_size, lock)

        self.compresslevel = compresslevel

    def _pre_value(self, value) -> bytes:

        value = Shock._pre_value(self, value)

        return gzip.compress(value, self.compresslevel)

    def _post_value(self, value: bytes):

        return gzip.decompress(value)


def open(
    file_path: str, flag: str = "r", map_size: int = 2**40, lock: bool = False, sync: bool = False, max_readers: int = 126):

    """
    Opens the database `file`.
    `flag`: r (read only, existing), w (read and write, existing),
            c (read, write, create if not exists), n (read, write, overwrite existing)
    `map_size`: Initial database size. Defaults to 2**40 (1TB).
    """

    return Shock(file_path, flag, map_size, lock, sync, max_readers)



# for key, value in shock0.items():
#     print(key)



# def multitest(db, key, data):
#     db[key] = data
    # db.sync()


# def multitest(file_path, key, data):
#     with open(file_path, 'w', lock=True) as db:
#         db[key] = data

























