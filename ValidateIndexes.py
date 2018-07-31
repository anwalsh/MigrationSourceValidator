"""
Project: MigrationSourceValidator
File: ValidateIndexes.py
Created: 2018-07-20T 1:34:49.211Z
WrittenBy: anwalsh
Last Modified: 2018-07-31T19:12:06.203Z
Revision: 1.0
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
        to_validate = dict(index for index in self._get_indices_from_payload())
        validated_index_payload = self._is_index_value_valid(to_validate)
        return validated_index_payload
        # return self._is_index_value_valid(to_validate)

    def _get_indices_from_payload(self):
        """
        """
        for _, value in self.s_namespaces.items():
            for index in value['indexes'].items():
                yield index

    def _is_index_value_valid(self, indices):
        """
        Validate that the index value is:
        - A number greater than 0
        - A number less than 0
        - An index is of a special type and the value specified is "text", "2d", or "hashed"

        Arguments:
        indices - a dictionary of the indices from the source replica set
        """
        special_case = ['text', '2dsphere', 'hashed']
        validity_outcome = {}

        for index_name, index_def in indices.items():
            validity_outcome.update({index_name: ""})
            # Iterate through index key definitions as a tuples
            for index_data in index_def['key']:
                index_value = index_data[1]
                if index_value not in special_case:
                    if index_value > 0 or index_value < 0:
                        validity_outcome[index_name] = "Valid"
                    else:
                        validity_outcome[index_name] = "Invalid"
                else:
                    validity_outcome[index_name] = "Special"

        return validity_outcome

    def _is_index_options_valid(self, indices):
        """
        Validate the options specified in the index:
        - TTL indexes must be single-field indexes with "expireAfteSeconds" defined
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

    def print_validated_indexes(self):
        """
        Prints the invalid indexes post validation
        """
        pprint(self.validated_indexes)
        # pprint(self.s_namespaces)

        # for key, value in self.s_namespaces.items():
        #     for index in value['indexes'].items():
        #         pprint(index)
