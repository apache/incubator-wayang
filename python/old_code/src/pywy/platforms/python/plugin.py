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
