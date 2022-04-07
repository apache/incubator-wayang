from typing import Dict
from pywy.operators.base import PywyOperator

class Mapping:
    mappings: Dict[str, type]

    def __init__(self):
        self.mappings = {}

    def add_mapping(self, operator: PywyOperator):
        self.mappings[operator.name_basic()] = type(operator)

    def get_instanceof(self, operator: PywyOperator):
        template = self.mappings[operator.name_basic()]
        if template is None:
            raise Exception(
                "the operator {} does not have valid mapping".format(
                    operator.name
                )
            )
        return template(operator)


    def __str__(self):
        return str(self.mappings)

    def __repr__(self):
        return self.__str__()