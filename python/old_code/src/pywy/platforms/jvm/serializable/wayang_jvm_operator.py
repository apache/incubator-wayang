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
from typing import Callable, List, TypeVar

from pywy.exception import PywyException


class WayangJVMOperator:

    kind: str
    name: str
    path: str
    udf: Callable

    previous: List['WayangJVMOperator']
    nexts: List['WayangJVMOperator']

    def __init__(self, kind, name):
        self.name = name
        self.kind = kind
        self.previous = []
        self.nexts = []

    def validate_vector(self, vect: List['WayangJVMOperator'], index: int, op: 'WayangJVMOperator' = None):
        if op is None:
            op = self

        if vect is None or len(vect) == 0:
            vect = [None] * (index + 1)

        if len(vect) < index:
            vect.extend([None for i in range(index + 1 - len(vect))])

        if vect[index] is not None:
            raise PywyException(
                'the position in the index "{}" is already in use for "{}" in the operator "{}"'.format(
                    index,
                    vect[index],
                    op
                )
            )

        return vect

    def connect_to(self, nexts_index: int, operator: 'WayangJVMOperator', previous_index: int) -> 'WayangJVMOperator':
        operator.previous = self.validate_vector(operator.previous, previous_index, operator)
        self.nexts = self.validate_vector(self.nexts, nexts_index)

        self.nexts[nexts_index] = operator
        operator.previous[previous_index] = self
        return self

    def __str__(self):
        return "WayangJVMOperator {}, previous.[{}], nexts.[{}]".format(
            self.name,
            self.previous,
            self.nexts
        )

    def is_source(self):
        return False

    def is_sink(self):
        return False

    def is_unary(self):
        return True

    def is_operator(self):
        return False

WJO_T = TypeVar('WJO_T', bound=WayangJVMOperator)


class WayangJVMMappartitionOperator(WayangJVMOperator):

    def __init__(self, name: str, udf: Callable = None):
        super().__init__("map_partition", name)
        self.udf = udf

    def is_operator(self):
        return True

class WayangJVMTextFileSource(WayangJVMOperator):

    def __init__(self, name: str, path: str):
        super().__init__("source", name)
        self.path = path

    def is_source(self):
        return True

class WayangJVMTextFileSink(WayangJVMOperator):

    def __init__(self, name: str, path: str):
        super().__init__("sink", name)
        self.path = path

    def is_sink(self):
        return True
