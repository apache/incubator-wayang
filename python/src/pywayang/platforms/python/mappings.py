from typing import Dict

from pywayang.operator.base import WyOperator
from pywayang.platforms.python.operators import *

class Mapping:
    mappings: Dict[str, type]

    def __init__(self):
        self.mappings = {}

    def add_mapping(self, operator: PythonExecutionOperator):
        self.mappings[operator.name] = type(operator)

    def get_instanceof(self, operator: WyOperator):
        template = self.mappings[operator.name]
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

OperatorMappings = Mapping()

OperatorMappings.add_mapping(PyFilterOperator())
OperatorMappings.add_mapping(PyTextFileSourceOperator())

