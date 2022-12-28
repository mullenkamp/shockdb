#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 25 21:21:11 2022

@author: mike
"""
from pickle import DEFAULT_PROTOCOL, HIGHEST_PROTOCOL, loads, dumps
import pathlib

#######################################################
### Parameters



#######################################################
### Functions

def move_cursor(txn):
    """
    Move the cursor past the encodings.
    """
    cursor = txn.cursor()
    cursor.set_key(b'01~._compressor')
    cursor.next()

    return cursor


def remove_db(file_path: str) -> None:

    fp = pathlib.Path(file_path)
    fp_lock = pathlib.Path(file_path + '-lock')

    fp.unlink(True)
    fp_lock.unlink(True)


def read_pkl_zstd(dctx, obj):
    """
    Deserializer from a pickled object compressed with zstandard.

    Parameters
    ----------
    obj : bytes or str
        Either a bytes object that has been pickled and compressed or a str path to the file object.

    Returns
    -------
    Python object
    """
    obj1 = dctx.decompress(obj)

    try:
        obj1 = loads(obj1)
    except:
        pass

    return obj1


def write_pkl_zstd(cctx, obj, compress_level=1, pkl_protocol=5):
    """
    Serializer using pickle and zstandard. Converts any object that can be pickled to a binary object then compresses it using zstandard. Optionally saves the object to disk. If obj is bytes, then it will only be compressed without pickling.

    Parameters
    ----------
    obj : any
        Any pickleable object.
    compress_level : int
        zstandard compression level.

    Returns
    -------
    If file_path is None, then it returns the byte object, else None.
    """
    if isinstance(obj, bytes):
        c_obj = cctx.compress(obj)
    else:
        c_obj = cctx.compress(dumps(obj, protocol=pkl_protocol))

    return c_obj





































































