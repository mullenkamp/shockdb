ShockDB
==================================

Introduction
------------
ShockDB uses `LMDB <http://www.lmdb.tech>`_ as the backend database, `python-lmdb <https://lmdb.readthedocs.io>`_ as the python bindings to the lmdb C library, and `lmdbm <https://github.com/Dobatymo/lmdb-python-dbm>`_ as a template to create a python key-value database in the style of `dbm <https://docs.python.org/3/library/dbm.html>`_ and `shelve <https://docs.python.org/3/library/shelve.html>`_. As such, the API uses all of the same python dictionary methods python programmers are used to.

Although lmdb and the associated low-level python bindings have a lot of advanced functionality, ShockDB was designed to be a simple fast single key-value database. Many default settings were made for this use case in mind. I've added several extra options in the open function for more advanced users.

Installation
------------
Install via pip::

  pip install shockdb

Or conda::

  conda install -c mullenkamp shockdb


I'll probably put it on conda-forge once I feel like it's up to an appropriate standard...


Serialization and compression
-----------------------------
The keys in Shockdb must always be strings. There's nothing you can do about this.
The values stored in lmdb must be bytes objects. But to be as flexible as possible (in the spirit of the shelve module), ShockDB provides multiple serialization options to convert values of many object types to bytes. The most obvious and flexible option is pickle. Two other options for serialization in ShockDB include the standard json and orjson. If your values can be serialized to json, then it is highly recommended to use the orjson option as it's really fast and you don't have to worry about pickle objects backwards compatibility issues.

ShockDB also provides several compression options for taking the serialized objects and compressing them into lmdb. These include gzip, zstd, and lz4. zstd and lz4 are very fast compressors and are the recommended compressors. Nevertheless, compression should be tested to verify that there is a significant reduction in file size as compared to no compression. If not, then you're wasting performance.

The defaults in ShockDB have been set to not do serialization and compression as to require the user to make a conscious decision on these options.

Custom user defined serializer and compressor classes can also be passed to the respective parameter names in the open function. Look at the main.py "Serializers and compressors" section for examples.

Usage
-----
The docstrings have a lot of info about the classes and methods. Files should be opened with the shockdb.open function. Read the docstrings of the open function for more details.

Write data
~~~~~~~~~~
.. code:: python

  import shockdb

  with shockdb.open('test.shock', 'n', serializer='pickle') as db:
    db['test_key'] = ['one', 2, 'three', 4]


Read data
~~~~~~~~~
.. code:: python

  with shockdb.open('test.shock', 'r') as db:
    test_data = db['test_key']

Notice that you don't need to pass serializer or compressor parameters when reading. ShockDB stores this info on the initial file creation.

Recommendations
~~~~~~~~~~~~~~~
In most cases, the user should use python's context manager "with" when reading and writing data. This will ensure data is properly written and (optionally) locks are released on the file. If the context manager is not used (and the default sync=False), then the user must be sure to run the db.sync() at the end of a series of writes to ensure the data has been fully written to disk. And as with other dbm style APIs, the db.close() must be run to close the file and release locks. MultiThreading is safe for multiple readers and writers, but only multiple readers are safe with MultiProcessing. If MultiThreading operations are expected, then opening a file with lock=True is necessary.


Benchmarks
-----------
Coming soon...

Possible future backends
------------------------
libmdbx
~~~~~~~
Supposedly an improvement over lmdb and subsequently meant to function similarly.
An important improvement is that databases can be reorganized in-place to reclaim space. lmdb requires you to copy an existing database to a new file to do this.

Main repo? But it is currently archived...:
https://github.com/erthink/libmdbx

Rough python bindings:
https://gitlab.com/Thermi/py3-libmdbx
