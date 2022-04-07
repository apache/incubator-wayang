from pywy.core import Executor
from pywy.platforms.python.execution import PyExecutor
from pywy.platforms.python.platform import PythonPlatform
from pywy.core import Plugin
from pywy.platforms.python.mappings import PYWY_OPERATOR_MAPPINGS


class PythonPlugin(Plugin):

    def __init__(self):
        super(PythonPlugin, self).__init__({PythonPlatform()}, PYWY_OPERATOR_MAPPINGS)

    def get_executor(self) -> Executor:
        return PyExecutor()
