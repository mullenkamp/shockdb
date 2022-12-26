#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 18:23:08 2022

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

tethys = Tethys([remote])

stns1 = tethys.get_stations(dataset_id)

station_ids = [s['station_id'] for s in stns1[:2]]
results1 = tethys.get_results(dataset_id, station_ids, heights=[10])




env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, subdir=False, sync=False)

txn = env.begin(write=True)

txn.put('results1'.encode(), pickle.dumps(results1))
txn.commit()

env1 = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, subdir=False, sync=False)

txn1 = env1.begin(write=True)

txn1.put('results2'.encode(), pickle.dumps(results1.sel(height=10))
txn1.commit()





remove_db(file_path)
with lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=False, create=True, subdir=False, sync=False) as env:
    for i in range(10000):
        with env.begin(write=True) as txn:
            key = 'data'+str(i)
            txn.put(key.encode(), pickle.dumps(np.arange(0, 100)))


env = lmdb.open(file_path, map_size=map_size, max_dbs=0, readonly=True, create=False, subdir=False, sync=False)

txn = env.begin(write=False)
d1 = pickle.loads(txn.get('data100'.encode()))







keys = [key for key in txn.cursor().iternext(keys=True, values=False)]



db = Lmdb.open('/home/mike/cache/test.db', 'n')

db['test'] = 'test'
db['results'] = pickle.dumps(results1)
db.sync()
db.close()

db = Lmdb.open('/home/mike/cache/test.db', 'r')

r1 = pickle.loads(db['results'])





db = SqliteDict("/home/mike/cache/example.sqlite")

db['results'] = results1

db.commit()

r1 = db['results']




def test_write_lmdb():
    with Lmdb.open('/home/mike/cache/test.db', 'n') as db:
        db['results1'] = pickle.dumps(results1)
        db['results2'] = pickle.dumps(results2)



def test_write_sqdict():
    with SqliteDict("/home/mike/cache/example.sqlite", flag='n', journal_mode='WAL') as db:
        db['results1'] = results1
        db['results2'] = results2
        db.commit()



def test_read_lmdb():
    with Lmdb.open('/home/mike/cache/test.db', 'r') as db:
        r1 = pickle.loads(db['results1'])
        r2 = pickle.loads(db['results2'])



def test_read_sqdict():
    with SqliteDict("/home/mike/cache/example.sqlite", flag='r', journal_mode='WAL') as db:
        r1 = db['results1']
        r2 = db['results2']






f = h5py.File('/home/mike/cache/test.h5', 'w')

ds = f.create_dataset('values', (10000), dtype=h5py.string_dtype())
for i in range(10000):
    ds[i] = b64encode(pickle.dumps(np.arange(0, 100)))

k = f.create_dataset('keys', (10000), dtype=h5py.string_dtype())
for i in range(10000):
    k[i] = str(i)




f = tables.open_file('/home/mike/cache/test.h5', 'w')
table = h5file.create_table('/', 'data', Particle)




f['test'] = b'test'
f['results'] = np.void(pickle.dumps(results1))

r1 = pickle.loads(f['results'][()].tobytes())


def test_write_h5():
    with h5py.File('/home/mike/cache/test.h5', 'w') as f:
        f['results1'] = np.void(pickle.dumps(results1))
        f['results2'] = np.void(pickle.dumps(results2))


def test_read_h5():
    with h5py.File('/home/mike/cache/test.h5', 'r') as f:
        r1 = pickle.loads(f['results1'][()].tobytes())
        r2 = pickle.loads(f['results2'][()].tobytes())


def test_write_seq_dict():
    db = {}
    for i in range(10000):
        db['data'+str(i)] = pickle.dumps(np.arange(0, 100))


def test_write_seq_shelflet():
    with shelflet.open('/home/mike/cache/test.shelf', 'n') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 100)


def test_write_big_shelflet():
    with shelflet.open('/home/mike/cache/test_big.shelf', 'n') as db:
        db['results1'] = results1


def test_write_seq_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'n') as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 100))
            db.sync()


def test_write_seq_lmdb():
    with Lmdb.open('/home/mike/cache/test.db', 'n') as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 100))


def test_write_seq_sqdict():
    with SqliteDict("/home/mike/cache/example.sqlite", flag='n', journal_mode='WAL') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 100)
            db.commit()


def test_write_seq_h5():
    with h5py.File('/home/mike/cache/test.h5', 'w') as db:
        for i in range(10000):
            db['data'+str(i)] = np.void(pickle.dumps(np.arange(0, 100)))


db = {'data'+str(i): pickle.dumps(np.arange(0, 100)) for i in range(10000)}
def test_read_seq_dict(db):
    for i in range(10000):
        n1 = pickle.loads(db['data'+str(i)])


def test_read_seq_lmdb():
    with Lmdb.open('/home/mike/cache/test.db', 'r') as db:
        for i in range(10000):
            n1 = pickle.loads(db['data'+str(i)])


def test_read_seq_shelflet():
    with shelflet.open('/home/mike/cache/test.shelf', 'r') as db:
        for i in range(10000):
            n1 = db['data'+str(i)]


def test_read_seq_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'r') as db:
        for i in range(10000):
            n1 = pickle.loads(db['data'+str(i)])


def test_read_seq_sqdict():
    with SqliteDict("/home/mike/cache/example.sqlite", flag='r', journal_mode='WAL') as db:
        for i in range(10000):
            n1 = db['data'+str(i)]


def test_read_seq_h5():
    with h5py.File('/home/mike/cache/test.h5', 'r') as db:
        for i in range(10000):
            n1 = pickle.loads(db['data'+str(i)][()].tobytes())


def test_write_bulk_dict():
    db = {'data'+str(i): pickle.dumps(np.arange(0, 100)) for i in range(10000)}


def test_write_bulk_lmdb():
    with Lmdb.open('/home/mike/cache/test.db', 'n') as db:
        db.update({'data'+str(i): pickle.dumps(np.arange(0, 100)) for i in range(10000)})


# def test_write_bulk_sqdict():
#     with SqliteDict("/home/mike/cache/example.sqlite", flag='n', journal_mode='WAL') as db:
#         for i in range(10000):
#             db['data'+str(i)] = np.arange(0, 100)


# def test_write_bulk_h5():
#     with h5py.File('/home/mike/cache/test.h5', 'w') as db:
#         for i in range(10000):
#             db['data'+str(i)] = np.void(pickle.dumps(np.arange(0, 100)))


def test_read_big_shelflet():
    with shelflet.open('/home/mike/cache/test_big.shelf', 'rf') as db:
        r1 = db['results1']









def test_write_big_shock():
    with open('/home/mike/cache/test_big.shock', 'n') as db:
        db['results1'] = pickle.dumps(results1)


def test_write_big_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'n') as db:
        db['results1'] = pickle.dumps(results1)

def test_read_big_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'r') as db:
        r1 = pickle.loads(db['results1'])


def test_write_seq_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'n') as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 100))
            db.sync()


def test_read_big_shock():
    with open('/home/mike/cache/test_big2.shock', 'r') as db:
        for i in range(200):
            r1 = db['data'+str(i)]


def test_write_bulk_shelve():
    with shelve.open('/home/mike/cache/test.shelve', 'n') as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 100))


def test_write_bulk_shock():
    with open('/home/mike/cache/test.shock', 'n', lock=False) as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 100))


def test_write_bulk_shock():
    with open('/home/mike/cache/test_big2.shock', 'n') as db:
        for i in range(200):
            db['data'+str(i)] = results1


db = open('/home/mike/cache/test_big2.shock', 'r')

with open('/home/mike/cache/test1.shock', 'r') as db:
    assert len(db) == 100
    for i in range(100):
        r1 = db['data'+str(i)]




























































