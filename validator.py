"""
Project: MigrationSourceValidator
File: validator.py
Created: 2018-07-25T13:46:38.214Z
WrittenBy: anwalsh
Last Modified: 2018-07-25T20:42:21.061Z
Revision: 1.0
Description: Main handler for the CLI validator workflow.
"""
import argparse
import mongo_connection as mc

parser = argparse.ArgumentParser(prog='validator', description = 'Pre-migration validation for the source MongoDB replica set')
parser.add_argument('uri', type=str, help='The source URI connections string')

args = parser.parse_args()

mc.GetNamespaces(mc.MakeConnection(args.uri))
