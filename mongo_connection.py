"""
Project: MigrationSourceValidator
File: mongo_connection.py
Created: 2018-07-25T14:04:08.539Z
WrittenBy: anwalsh
Last Modified: 2018-07-25T20:42:31.305Z
Revision: 1.0
Description: Handles connecting to the MongoDB Host and associated fault tolerance.
"""
from pymongo import MongoClient
import json

def MakeConnection(c):
    return MongoClient(c)

def GetNamespaces(c):
    namespaces = dict((db, [coll for coll in c[db].collection_names()])
                      for db in c.database_names() if db not in ('admin', 'local'))


    print(json.dumps(namespaces, indent = 4))

