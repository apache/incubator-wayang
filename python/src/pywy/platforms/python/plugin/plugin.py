from pywy.platforms.basic.executor import Executor
from pywy.platforms.python.execution.executor import PyExecutor
from pywy.platforms.python.platform import PythonPlatform
from pywy.platforms.basic.plugin import Plugin
from pywy.platforms.python.mappings import PywyOperatorMappings


class PythonPlugin(Plugin):

    def __init__(self):
        super(PythonPlugin, self).__init__({PythonPlatform()}, PywyOperatorMappings)

    def get_executor(self) -> Executor:
        return PyExecutor()