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
                    'optionsValid': self._is_index_options_valid(index_def),
                    'buildType': self._index_build(index_def),
                }
            })
        return validated_indices

    def _get_indices_from_payload(self):
        """
        Get the indices from the SourceNamespaces s_namespace object
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

        Validate that the key is:
        - Less than 1024 bytes ref: https://docs.mongodb.com/manual/reference/limits/#Index-Key-Limit

        Arguments:
        index_def - dictionary containing the index definitions from the source for value validation
        """
        special_case = ['text', '2dsphere', 'hashed', '2d']
        valid = True

        for index_data in index_def['key']:
            index_key = index_data[0]
            index_value = index_data[1]
            if index_value not in special_case:
                if 1024 > len(str.encode(index_key)):
                    if type(index_value) == float or type(index_value) == int:
                        valid = True
                    else:
                        valid = False
                        return valid
                else:
                    valid = False
                    return valid
            elif index_value in special_case:
                return index_value
            else:
                return "Not Validated"
        return valid

    def _index_build(self, index_def):
        """
        Validate the index definition will build in the foreground or the background

        Arguments:
        index_def - a dictionary contianing the index definitions from the source for validation
        """
        for index_data in index_def['key']:
            if index_data[0] == '_id' and len(index_def['key']) == 1:
                return 'Default Index'
            else:
                if index_def.get('background') == True:
                    return 'Background'
                else:
                    return 'Foreground'

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
        index_def - a dictionary containing the index definitions from the source for option validation
        """
        valid_option = True
        options = {
            'background', 'ns', 'v', 'key', 'unique', '2dsphereIndexVersion',
            'sparse', 'unique', 'partialFilterExpression', 'collation',
            'expireAfterSeconds'
        }
        if index_def.get('expiresAfterSeconds') != None:
            if len(index_def['key']) > 1:
                valid_option = False
                return valid_option
        else:
            for index_option in index_def.keys():
                if index_option not in options:
                    valid_option = False
                    return valid_option
                else:
                    valid_option = True
        return valid_option

    def print_validated_indexes(self):
        """
        Prints the invalid indexes post validation
        """
        pprint(self.validated_indexes)
