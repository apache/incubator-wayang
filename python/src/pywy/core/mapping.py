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

from typing import Dict
from pywy.operators.base import PywyOperator


class Mapping:
    mappings: Dict[str, type]

    def __init__(self):
        self.mappings = {}

    def add_mapping(self, operator: PywyOperator):
        self.mappings[operator.name_basic()] = type(operator)

    def get_instanceof(self, operator: PywyOperator):
        template = self.mappings[operator.name_basic()]
        if template is None:
            raise Exception(
                "the operator {} does not have valid mapping".format(
                    operator.name
                )
            )
        return template(operator)

    def __str__(self):
        return str(self.mappings)

    def __repr__(self):
        return self.__str__()
