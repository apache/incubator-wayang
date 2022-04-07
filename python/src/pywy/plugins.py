from pywy.core.platform import Platform
from pywy.core import Plugin
from pywy.platforms.python.plugin import PythonPlugin

# define the basic plugins that can be used
JAVA = Plugin({Platform('java')})
SPARK = Plugin({Platform('spark')})
FLINK = Plugin({Platform('flink')})
# plugin for the python platform
PYTHON = PythonPlugin()
