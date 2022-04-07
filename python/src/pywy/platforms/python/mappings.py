from pywy.core import Mapping
from pywy.platforms.python.operator import *


PywyOperatorMappings = Mapping()

PywyOperatorMappings.add_mapping(PyFilterOperator())
PywyOperatorMappings.add_mapping(PyTextFileSourceOperator())
PywyOperatorMappings.add_mapping(PyTextFileSinkOperator())

