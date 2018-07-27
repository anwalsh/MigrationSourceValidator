"""
Project: MigrationSourceValidator
File: mongo_connection.py
Created: 2018-07-25T14:04:08.539Z
WrittenBy: anwalsh
Last Modified: 2018-07-27T20:59:59.261Z
Revision: 1.16
Description: Handles connecting to the MongoDB Host and associated fault tolerance.
"""
from pymongo import MongoClient

def MakeConnection(c):
    return MongoClient(c)
