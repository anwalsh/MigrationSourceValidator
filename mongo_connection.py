"""
Project: MigrationSourceValidator
File: mongo_connection.py
Created: 2018-07-25T14:04:08.539Z
WrittenBy: anwalsh
Last Modified: 2018-07-27T17:49:07.603Z
Revision: 1.16
Description: Handles connecting to the MongoDB Host and associated fault tolerance.
"""
from pprint import pprint
from pymongo import MongoClient

def MakeConnection(c):
    return MongoClient(c)

def GetNamespaces(c):
    namespaces = dict((db, [coll for coll in GenGetCollections(c, db)])
                        for db in c.database_names() if db not in ('admin', 'local', 'config'))

    pprint(namespaces, indent = 4)

def GenGetDBStats(c, db):
    yield c[db].command({'dbstats' : 1, 'scale' : 1024})

def GenGetCollections(c, db):
    for coll in c[db].collection_names():
        data = c[db].command('collstats', coll, scale=1024)
        target = {data.get('ns'): {'count': data.get('count'), 'size': data.get('size'),
                                    'avgObjSize': data.get('avgObjSize'), 'capped': data.get('capped'),
                                    'nindexes': data.get('nindexes'), 'totalIndexSize': data.get('totalIndexSize'),
                                    'indexSizes': data.get('indexSizes'), 'Indexes': GetIndexes(c, db, coll)}}
        yield target

def GetIndexes(c, db, coll):
    indexes = c[db][coll].index_information()

    return indexes
