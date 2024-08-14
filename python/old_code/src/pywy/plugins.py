#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from pywy.core.platform import Platform
from pywy.core import Plugin
from pywy.platforms.jvm.plugin import JVMPlugin
from pywy.platforms.python.plugin import PythonPlugin

# define the basic plugins that can be used
JAVA = Plugin({Platform('java')})
SPARK = Plugin({Platform('spark')})
FLINK = Plugin({Platform('flink')})
# plugin for the python platform
PYTHON = PythonPlugin()
JVMs = JVMPlugin()
