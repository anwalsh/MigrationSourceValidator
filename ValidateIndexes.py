"""
Project: MigrationSourceValidator
File: ValidateIndexes.py
Created: 2018-07-20T 1:34:49.211Z
WrittenBy: anwalsh
Last Modified: 2018-08-05 T21:26:06.526  
Revision: 2.0
Description: Class to validate the source indexes are in compliance with MongoDB 3.4 Stricter Index Validation: https://docs.mongodb.com/manual/release-notes/3.4-compatibility/#stricter-validation-of-collection-and-index-specifications
"""
from pprint import pprint


class ValidateIndexes:
    """
    Class to validate indexes meet the 3.4 stricter index validation
    """

    def __init__(self, namespace_topology):
        """
        Create the ValidateIndexes class object
        """
        self.s_namespaces = namespace_topology
        self.validated_indexes = self._get_index_validity()

    def _get_index_validity(self):
        """
        Handler for index validation
        """
        indices = dict(index for index in self._get_indices_from_payload())
        validated_indices = {}

        for index_name, index_def in indices.items():
            validated_indices.update({
                index_name: {
                    'valueValid': self._is_index_value_valid(index_def),
                    'optionsValid': self._is_index_options_valid(index_def)
                }
            })

        return validated_indices

    def _get_indices_from_payload(self):
        """
        """
        for _, value in self.s_namespaces.items():
            for index in value['indexes'].items():
                yield index

    def _is_index_value_valid(self, index_def):
        """
        Validate that the index value is:
        - A number greater than 0
        - A number less than 0
        - An index is of a special type and the value specified is "text", "2d", or "hashed"

        Arguments:
        index_def - dictionary containing the index definitions from source for value validation
        """
        special_case = ['text', '2dsphere', 'hashed']
        for index_data in index_def.get('key'):
            index_value = index_data[1]
            if index_value not in special_case:
                if index_value > 0 or index_value < 0:
                    return "Valid"
                else:
                    return "Invalid"
            elif index_value == None:
                return None
            else:
                return "Special"

    def _is_index_options_valid(self, index_def):
        """
        Validate the options specified in the index:
        - TTL indexes must be single-field indexes with "expireAfterSeconds" defined
        - unique : true
        - partialFilterExpression defined with:
            - equality expressions
            - $exists : true expression
            - $gt, $gte, $lt, $lte, expressions
            - $type expressions
            - $and operator at the top-level only
        - collation option
        - sparse : true option

        Arguments:
        indices - a dictionary of the indices from the source replica set
        """
        # pprint(index_def)

    def print_validated_indexes(self):
        """
        Prints the invalid indexes post validation
        """
        pprint(self.validated_indexes)
        # pprint(self.s_namespaces)

        # for key, value in self.s_namespaces.items():
        #     for index in value['indexes'].items():
        #         pprint(index)
