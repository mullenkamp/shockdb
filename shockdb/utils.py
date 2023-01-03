#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 25 21:21:11 2022

@author: mike
"""
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
    cursor.set_key(b'02~._key_serializer')
    cursor.next()

    return cursor


def remove_db(file_path: str) -> None:

    fp = pathlib.Path(file_path)
    fp_lock = pathlib.Path(file_path + '-lock')

    fp.unlink(True)
    fp_lock.unlink(True)






































































