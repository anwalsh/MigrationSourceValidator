"""
Project: MigrationSourceValidator
File: validator.py
Created: 2018-07-19T13:46:38.214Z
WrittenBy: anwalsh
Last Modified: 2018-07-27T22:15:39.361Z
Revision: 2.0
Description: Main handler for the CLI validator workflow.
"""
import argparse
import SourceNamespaces as sn
import ValidateIndexes as vi

parser = argparse.ArgumentParser(
    prog='validator',
    description='Pre-migration validation for the source MongoDB replica set')
parser.add_argument('uri', type=str, help='The source URI connections string')
parser.add_argument(
    '--filetype', '-f', help='Desired output type: stdout(default), JSON')

args = parser.parse_args()
s_topology = sn.SourceNamespaces(args.uri)
top = vi.ValidateIndexes(s_topology.namespaces)

if args.filetype == 'json':
    s_topology.write_json_to_file()
else:
    top.print_invalid_indexes()
    # s_topology.print_namespaces()
