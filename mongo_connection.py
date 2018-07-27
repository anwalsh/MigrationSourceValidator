"""
Project: MigrationSourceValidator
File: mongo_connection.py
Created: 2018-07-25T14:04:08.539Z
WrittenBy: anwalsh
Last Modified: 2018-07-25T20:42:31.305Z
Revision: 1.0
Description: Handles connecting to the MongoDB Host and associated fault tolerance.
"""
from pprint import pprint
from pymongo import MongoClient

def MakeConnection(c):
    return MongoClient(c)

def GetNamespaces(c):
    namespaces = dict((db, [coll for coll in GenGetCollStats(c, db)])
                        for db in c.database_names() if db not in ('admin', 'local', 'config'))
    pprint(namespaces, indent = 4)

    for cstat in GenGetCollStats(c, 'yelp'):
        pprint(cstat, indent = 4)
    
def GenGetDBStats(c, db):
    yield c[db].command({'dbstats' : 1, "size" : 1024})

def GenGetCollStats(c, db):
    for coll in c[db].collection_names():
        data = c.yelp.command('collstats', coll)
        target = {data.get('ns'): [{'count': data.get('count'), 'size': data.get('size'),
                                    'avgObjSize': data.get('avgObjSize'), 'capped': data.get('capped'),
                                    'nindexes': data.get('nindexes'), 'totalIndexSize': data.get('totalIndexSize'),
                                    'indexSizes': data.get('indexSizes')}]}
        yield target
