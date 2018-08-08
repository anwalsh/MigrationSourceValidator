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
    top.print_validated_indexes()

# TODO: accept other types of input outside of URI connection string or SRV. JS files, local instances of MongoDB, mongodumps[?], etc.
# TODO: Test hostname:port and allow hostname port to be provided with the appropriate options
