#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 18:23:08 2022

@author: mike
"""
from tethysts import Tethys
import io
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
import shockdb
import booklet

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
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer=None) as db:
        for i in range(10000):
            db['data'+str(i)] = pickle.dumps(np.arange(0, 1000))


def test_write_bulk_shock():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 1000)


def test_write_bulk_shock():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer='orjson') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 1000)


def test_write_bulk_shock():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='orjson') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 1000)


def test_write_bulk_shock():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = np.arange(0, 1000)



def test_write_bulk_shock():
    with open('/home/mike/cache/test_big2.shock', 'n', compressor=None) as db:
        for i in range(200):
            db['data'+str(i)] = results1


db = open('/home/mike/cache/test_big2.shock', 'r')


def test_read_bulk_shock():
    with open('/home/mike/cache/test.shock', 'r', compressor='zstd', serializer='orjson') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]






value = pickle.dumps(np.arange(0, 1000))
def test_write_shock_none_none():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer=None) as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000)
def test_write_shock_none_pickle():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = value


value = np.arange(0, 1000).tolist()
def test_write_shock_none_json():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer='json') as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000)
def test_write_shock_none_orjson():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=None, serializer='orjson') as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = pickle.dumps(np.arange(0, 1000))
def test_write_shock_zstd_none():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer=None) as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000)
def test_write_shock_zstd_pickle():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000)
def test_write_shock_lz4_pickle():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='lz4', serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000).tolist()
def test_write_shock_zstd_json():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='json') as db:
        for i in range(10000):
            db['data'+str(i)] = value

value = np.arange(0, 1000)
def test_write_shock_zstd_orjson():
    with open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='orjson') as db:
        for i in range(10000):
            db['data'+str(i)] = value

## Read
def test_read_shock_none_none():
    with open('/home/mike/cache/test.shock', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_none_pickle():
    with open('/home/mike/cache/test.shock', 'r', compressor=None, serializer='pickle') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_none_json():
    with open('/home/mike/cache/test.shock', 'r', compressor=None, serializer='json') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_none_orjson():
    with open('/home/mike/cache/test.shock', 'r', compressor=None, serializer='orjson') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_zstd_none():
    with open('/home/mike/cache/test.shock', 'r', compressor='zstd', serializer=None) as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_zstd_pickle():
    with open('/home/mike/cache/test.shock', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_lz4_pickle():
    with open('/home/mike/cache/test.shock', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_zstd_json():
    with open('/home/mike/cache/test.shock', 'r', compressor='zstd', serializer='json') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]

def test_read_shock_zstd_orjson():
    with open('/home/mike/cache/test.shock', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]


db = open('/home/mike/cache/test.shock', 'r')

for key, value in db.items():
    print(key)

db = open('/home/mike/cache/test.shock', 'n', lock=False, compressor='zstd', serializer='orjson')


with open('/home/mike/cache/test.shock', 'n', lock=False, compressor=Zstd, serializer=Orjson) as db:
    for i in range(10000):
        db['data'+str(i)] = value




value = pickle.dumps(np.arange(0, 1000))
def test_write_shelflet_none_none():
    with open('/media/nvme1/cache/arete/test.shelf', 'n', value_serializer=None, key_serializer='str') as db:
        for i in range(10000):
            db['data'+str(i)] = value


value = np.arange(0, 1000)
def test_write_shelflet_zstd_pickle():
    with open('/home/mike/cache/test.shelf', 'n', compressor='zstd', serializer='pickle') as db:
        for i in range(10000):
            db['data'+str(i)] = value



def test_read_shelflet_zstd_pickle():
    with open('/home/mike/cache/test.shelf', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]



db = Arete('/media/nvme1/cache/arete/test.arete', 'r')






import numpy as np

value = np.arange(0, 1000)
def test_write_booklet_none_none():
    with open('/home/mike/cache/test.blt', 'n', key_serializer='uint4', value_serializer='numpy_int2_zstd') as db:
        for i in range(10000):
            value = np.arange(0, i+1)
            db[i] = value

def test_read_booklet():
    with open('/home/mike/cache/test.blt', 'r') as db:
        for i in range(10000):
            r1 = db['data'+str(i)]


def test_read_booklet_iter():
    with booklet.open('/home/mike/cache/test.book', 'r') as db:
        for i, v in db.items():
            r1 = v


key1 = 3050201
key2 = 7244542
db = open('/home/mike/cache/test.blt')
db[key1]


with open('/home/mike/cache/test.blt', 'n', key_serializer='uint4', value_serializer='gpd_zstd', n_buckets=1600) as f:
    for way_id, branches in catches_minor_dict.items():
        if way_id in [key1, key2]:
            print(way_id)
            f[way_id] = branches

f1 = db[key]
for k in f1.nzsegment:
    if k in catches_minor_dict:
        d




import orjson
import zstandard as zstd

class OrjsonZstd:
  def dumps(obj) -> bytes:
      return zstd.compress(orjson.dumps(obj, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_SERIALIZE_NUMPY))
  def loads(obj: bytes):
      return orjson.loads(zstd.decompress(obj))

with booklet.open('test.book', 'n', value_serializer=OrjsonZstd, key_serializer='str') as db:
  db['big_test'] = list(range(1000000))

with booklet.open('test.book', 'r') as db:
  big_test_data = db['big_test']



start = time()
with open('/media/nvme1/git/nzrec/data/node.blt') as node:
    for k in node.keys():
        r1 = node[k]
end = time()
print(end - start)



node = booklet.open('/media/nvme1/git/nzrec/data/node.arete')

value = pickle.dumps(np.arange(0, 110))

start = time()
with open('/media/nvme1/data/NIWA/test/test_file.tmp', 'wb', buffering=500000) as f:
    for i in range(50000):
        f.write(value)

end = time()
print(end - start)


def write_buffered():
    with open('/media/nvme1/data/NIWA/test/test_file.tmp', 'wb', buffering=500000) as f:
        for i in range(50000):
            f.write(value)


def write_unbuffered():
    with open('/media/nvme1/data/NIWA/test/test_file.tmp', 'wb', buffering=0) as f:
        for i in range(50000):
            f.write(value)


value_len = len(value)
def write_buffered_mmap():
    with open('/media/nvme1/data/NIWA/test/test_file.tmp', 'wb', buffering=500000) as f:
        f.write(value)
    with open('/media/nvme1/data/NIWA/test/test_file.tmp', 'r+b', buffering=500000) as f:
        mm = mmap.mmap(f.fileno(), 0)
        size = value_len
        for i in range(50000):
            size += value_len
            mm.resize(size)
            mm.write(value)



with open("hello.txt", "wb") as f:
    f.write(b"Hello Python\n")

with open("hello.txt", "r+b") as f:
    with mmap.mmap(f.fileno(), 0) as mm:
        print(mm.readline())  # prints b"Hello Python!\n"
        print(mm[:5])  # prints b"Hello"
        mm.seek(5)
        mm.write(b" world!\n")
        mm.seek(0)
        print(mm.readline())  # prints b"Hello world!\n”
        mm.resize(17)
        mm.move(10, 6, len(b'world!\n'))
        mm[6:10] = b'big '
        print(mm[:])  # prints b'Hello big world!\n'













































