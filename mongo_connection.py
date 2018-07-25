"""
Project: MigrationSourceValidator
File: mongo_connection.py
Created: 2018-07-25T14:04:08.539Z
WrittenBy: anwalsh
Last Modified: 
Revision: 1.0
Description: Handles connecting to the MongoDB Host and associated fault tolerance.
"""
from pymongo import *

def MakeConnection(c):
    print("Successfully created the MongoClient object")
    print(c)
    client = MongoClient(c)
    print("Make connection...")
    try:
        # The ismaster command is cheap and does not require auth.
        print(client.admin.command('ismaster'))
    except errors.ConnectionFailure:
        print("Server not available")
