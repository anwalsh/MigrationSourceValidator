"""
Project: MigrationSourceValidator
File: SourceNamespaces.py
Created: 2018-07-27T19:16:17.492Z
WrittenBy: anwalsh
Last Modified: 2018-07-27T22:16:02.462Z
Revision: 3.0
Description: Class to encapsulate source namespaces and the applicable methods for their population.
"""
import json
from pprint import pprint
from pymongo import MongoClient

class SourceNamespaces:
    """
    Sourcenamespaces class which takes a URI or SRV Connection string as an argument
    """
    def __init__(self, connection_string):
        """
        Create the class object
        """
        self.client = MongoClient(connection_string)
        self.namespaces = self._get_namespaces()

    def _get_namespaces(self):
        """Initializes the namespace dictionary with the source namespaces and calls GenGetCollections()
        to add a nested dictionary of specific collstats and index shapes

        Arguments:
        self
        """
        namespaces = dict((db, [coll for coll in self.gen_get_collections(db)])
                           for db in self.client.database_names() if db not in ('admin', 'local', 'config'))
        return namespaces

    def gen_get_DBstats(self, db):
        """
        Retrieves the dbstats at KB resolution from the provide MongoDB instance(s)

        Arguments:
        self
        db -- string, database name
        """
        yield self.client[db].command('dbstats', scale=1024)

    def gen_get_collections(self, db):
        """
        Generator yields the statistics and index definitions at KB scale from the provided database to be added
        to the namespace dictionary on a per collection basis

        Arguments:
        self
        db -- string, database name
        """
        for coll in self.client[db].collection_names():
            data = self.client[db].command('collstats', coll, scale=1024)
            target = {data.get('ns'): {'count': data.get('count'), 'size': data.get('size'),
                                    'avgObjSize': data.get('avgObjSize'), 'capped': data.get('capped'),
                                    'nindexes': data.get('nindexes'), 'totalIndexSize': data.get('totalIndexSize'),
                                    'indexSizes': data.get('indexSizes'), 'Indexes': self.get_indexes(db, coll)}}
            yield target

    def get_indexes(self, db, coll):
        """
        Gathers index structure for a particular namespace into a dictionary

        Arguments:
        self
        db -- string, database name
        coll -- string collection name
        """
        return self.client[db][coll].index_information()

    def print_namespaces(self):
        """
        Prints the namespaces dictionary

        Arguments:
        self
        """
        pprint(self.namespaces)

    def write_json_to_file(self):
        """
        Writes the namespaces dictionary to a JSON file within the current working directory
        """
        with open('data.json', 'w') as outfile:
            json.dump(self.namespaces, outfile)
