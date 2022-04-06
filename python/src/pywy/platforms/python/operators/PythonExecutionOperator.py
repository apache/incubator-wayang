from pywy.wayangplan.base import PywyOperator
from pywy.platforms.python.channels import Channel

class PythonExecutionOperator(PywyOperator):

    def prefix(self) -> str:
        return 'Py'

    def execute(self, inputs: Channel, output: Channel):
        pass