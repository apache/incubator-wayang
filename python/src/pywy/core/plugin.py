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

from typing import Set

from pywy.core.executor import Executor
from pywy.core.platform import Platform
from pywy.core.mapping import Mapping


class Plugin:
    """
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms: Set[Platform]
    mappings: Mapping

    def __init__(self, platforms: Set[Platform], mappings: Mapping = Mapping()):
        self.platforms = platforms
        self.mappings = mappings

    def get_mappings(self) -> Mapping:
        return self.mappings

    def get_executor(self) -> Executor:
        pass

    def __str__(self):
        return "Platforms: {}, Mappings: {}".format(str(self.platforms), str(self.mappings))

    def __repr__(self):
        return self.__str__()
