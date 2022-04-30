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
from pywy.core.core import Plugin
from pywy.platforms.jvm.context import JVMTranslateContext
from pywy.platforms.jvm.execution import JVMExecutor
from pywy.platforms.jvm.mappings import JVM_OPERATOR_MAPPINGS
from pywy.platforms.jvm.platform import JVMPlatform


class JVMPlugin(Plugin):

    def __init__(self):
        super(JVMPlugin, self).__init__({JVMPlatform()}, JVM_OPERATOR_MAPPINGS, JVMTranslateContext())

    def get_executor(self) -> Executor:
        return JVMExecutor()
