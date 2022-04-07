from pywy.core import Executor
from pywy.platforms.python.execution.executor import PyExecutor
from pywy.platforms.python.platform import PythonPlatform
from pywy.core import Plugin
from pywy.platforms.python.mappings import PywyOperatorMappings


class PythonPlugin(Plugin):

    def __init__(self):
        super(PythonPlugin, self).__init__({PythonPlatform()}, PywyOperatorMappings)

    def get_executor(self) -> Executor:
        return PyExecutor()