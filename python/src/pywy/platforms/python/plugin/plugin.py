from pywy.platforms.python.platform import PythonPlatform
from pywy.platforms.basic.plugin import Plugin
from pywy.platforms.python.mappings import PywyOperatorMappings


class PythonPlugin(Plugin):

    def __init__(self):
        super(PythonPlugin, self).__init__({PythonPlatform()}, PywyOperatorMappings)