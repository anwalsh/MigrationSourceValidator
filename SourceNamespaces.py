"""
Project: MigrationSourceValidator
File: SourceNamespaces.py
Created: 2018-07-27T19:16:17.492Z
WrittenBy: anwalsh
Last Modified: 2018-07-27T20:59:46.142Z
Revision: 1.0
Description: Class to encapsulate source namespaces and the applicable methods for their population.
"""
from pprint import pprint
from pymongo import MongoClient

class SourceNamespaces:

    namespaces = {}

    def __init__(self, c):
        """
        Create the class object
        """
        self.c = c
        self.GetNamespaces(c)

    def GetNamespaces(self, c):
        """Initializes the namespace dictionary with the source namespaces and calls GenGetCollections()
        to add a nested dictionary of specific collstats and index shapes

        Arguments:
        self
        c -- MongoClient from mongo_connection.py
        """
        self.namespaces = ((db, [coll for coll in self.GenGetCollections(c, db)])
                           for db in c.database_names() if db not in ('admin', 'local', 'config'))

    def GenGetDBStats(self, c, db):
        """
        Retrieves the dbstats at KB resolution from the provide MongoDB instance(s)

        Arguments:
        self
        c -- MongoClient from mongo_connection.py which is passed in at class creation
        db -- string, database
        """
        yield c[db].command('dbstats', scale=1024)

    def GenGetCollections(self, c, db):
        """
        Generator yields the statistics and index definitions at KB scale from the provided database to be added
        to the namespace dictionary on a per collection basis

        Arguments:
        self
        c -- MongoClient from mongo_connection.py which is passed in at class creation
        db -- string, database
        """
        for coll in c[db].collection_names():
            data = c[db].command('collstats', coll, scale=1024)
            target = {data.get('ns'): {'count': data.get('count'), 'size': data.get('size'),
                                    'avgObjSize': data.get('avgObjSize'), 'capped': data.get('capped'),
                                    'nindexes': data.get('nindexes'), 'totalIndexSize': data.get('totalIndexSize'),
                                    'indexSizes': data.get('indexSizes'), 'Indexes': self.GetIndexes(c, db, coll)}}
            yield target

    def GetIndexes(self, c, db, coll):
        """
        Gathers index structure for a particular namespace into a dictionary


        """
        indexes = c[db][coll].index_information()

        return indexes

    def PrintNamespaces(self):
        """
        Prints the namespaces dictionary

        Arguments:
        self
        """
        for ns in self.namespaces:
            pprint(ns, indent=4)
