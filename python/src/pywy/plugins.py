from pywy.platforms.basic.platform import Platform
from pywy.platforms.basic.plugin import Plugin
from pywy.platforms.python.plugin import PythonPlugin

# define the basic plugins that can be used
java = Plugin(Platform('java'))
spark = Plugin(Platform('spark'))
flink = Plugin(Platform('flink'))
# plugin for the python platform
python = PythonPlugin()
