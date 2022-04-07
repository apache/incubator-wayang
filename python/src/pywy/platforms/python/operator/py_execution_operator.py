from pywy.operators.base import PywyOperator
from pywy.platforms.python.channels import Channel

class PyExecutionOperator(PywyOperator):

    def prefix(self) -> str:
        return 'Py'

    def execute(self, inputs: Channel, output: Channel):
        pass