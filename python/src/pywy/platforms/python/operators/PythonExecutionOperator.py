from pywy.wayangplan.base import WyOperator
from pywy.platforms.python.channels import Channel

class PythonExecutionOperator(WyOperator):

    def prefix(self) -> str:
        return 'Py'

    def execute(self, inputs: Channel, output: Channel):
        pass