#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 21:10:00 2022

@author: mike
"""
from tethysts import Tethys
import io
from lmdbm import Lmdb
from sqlitedict import SqliteDict
import shelflet
import h5py
import shelve
from base64 import b64decode, b64encode
import tables
import dbm
import pickle
import numpy as np
import lmdb
import pathlib
import concurrent.futures
import multiprocessing as mp

import main

############################################################
### Parameters

remote = {'bucket': 'nz-open-modelling-consortium', 'public_url': 'https://b2.nzrivers.xyz/file/', 'version': 4}

dataset_id = '7751c5f1bf47867fb109d7eb'

file_path = '/home/mike/cache/test1.shock'
map_size = 2**40

def remove_db(file_path: str) -> None:

    fp = pathlib.Path(file_path)
    fp_lock = pathlib.Path(file_path + '-lock')

    fp.unlink(True)
    fp_lock.unlink(True)


##########################################################
### Testing

tethys = Tethys([remote], cache='/home/mike/cache')

stns1 = tethys.get_stations(dataset_id)

station_ids = [s['station_id'] for s in stns1[:1]]
results1 = tethys.get_results(dataset_id, station_ids, heights=[10])










if __name__ == '__main__':

    # db = main.open(file_path, 'n', lock=True)
    # for i in range(100):
    #     key = 'data'+str(i)
    #     main.multitest(db, key, results1)
    # db.close()

    with concurrent.futures.ProcessPoolExecutor(max_workers=3, mp_context=mp.get_context("spawn")) as executor:
        db = main.open(file_path, 'n', lock=True)
        db.close()
        futures = []
        for i in range(100):
            key = 'data'+str(i)
            f = executor.submit(main.multitest, file_path, key, results1)
            futures.append(f)
        runs = concurrent.futures.wait(futures)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    #     db = main.open(file_path, 'n', lock=True)
    #     futures = []
    #     for i in range(100):
    #         key = 'data'+str(i)
    #         f = executor.submit(main.multitest, db, key, results1)
    #         futures.append(f)
    #     runs = concurrent.futures.wait(futures)
    #     db.close()




















