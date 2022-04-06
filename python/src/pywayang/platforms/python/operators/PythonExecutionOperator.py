from pywayang.operator.base import WyOperator
from pywayang.platforms.python.channels import Channel

class PythonExecutionOperator(WyOperator):

    def execute(self, inputs: Channel, output: Channel):
        pass