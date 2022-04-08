from typing import List, Type

from pywy.core.channel import CH_T
from pywy.operators.base import PywyOperator


class PyExecutionOperator(PywyOperator):

    def prefix(self) -> str:
        return 'Py'

    def execute(self, inputs: List[Type[CH_T]], output: List[CH_T]):
        pass
