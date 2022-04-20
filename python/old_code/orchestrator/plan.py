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

import logging
from enum import Enum

class Descriptor:

    def __init__(self):
        self.sinks = []
        self.sources = []
        self.boundary_operators = None
        logging.basicConfig(filename='../config/execution.log', level=logging.DEBUG)
        self.plugins = []

    class Plugin(Enum):
        java = 0
        spark = 1

    def get_boundary_operators(self):
        return self.boundary_operators

    def add_source(self, operator):
        self.sources.append(operator)

    def get_sources(self):
        return self.sources

    def add_sink(self, operator):
        self.sinks.append(operator)

    def get_sinks(self):
        return self.sinks

    def add_plugin(self, plugin):
        self.plugins.append(plugin)

    def get_plugins(self):
        return self.plugins
