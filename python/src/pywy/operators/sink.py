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

from typing import Any
from pywy.types import GenericTco
from pywy.operators.base import PywyOperator


class SinkOperator(PywyOperator):

    def postfix(self) -> str:
        return 'Sink'


class SinkUnaryOperator(SinkOperator):

    def __init__(self, name: str, input_type: GenericTco = Any):
        super().__init__(name, "output", input_type, None, 1, 0)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class TextFileSink(SinkUnaryOperator):
    path: str
    json_name: str

    def __init__(self, path: str, input_type: GenericTco):
        super().__init__('TextFile', input_type)
        self.path = path
        self.json_name = "textFileOutput"

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
