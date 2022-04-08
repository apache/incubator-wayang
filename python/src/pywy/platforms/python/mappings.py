from pywy.core import Mapping
from pywy.platforms.python.operator import *


PYWY_OPERATOR_MAPPINGS = Mapping()

PYWY_OPERATOR_MAPPINGS.add_mapping(PyFilterOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyTextFileSourceOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyTextFileSinkOperator())
PYWY_OPERATOR_MAPPINGS.add_mapping(PyMapOperator())

