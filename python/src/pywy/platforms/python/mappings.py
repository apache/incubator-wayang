from pywy.platforms.basic.mapping import Mapping
from pywy.platforms.python.operators import *


PywyOperatorMappings = Mapping()

PywyOperatorMappings.add_mapping(PyFilterOperator())
PywyOperatorMappings.add_mapping(PyTextFileSourceOperator())
PywyOperatorMappings.add_mapping(PyTextFileSinkOperator())

