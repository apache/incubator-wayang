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

from typing import Set, List, cast

from pywy.core.core import Plugin, PywyPlan
from pywy.operators.base import PO_T
from pywy.types import (GenericTco, Predicate, Function, FlatmapFunction, IterableOut, T, In, Out)
from pywy.operators import *


class WayangContext:
    """
    This is the entry point for users to work with Wayang.
    """
    plugins: Set[Plugin]

    def __init__(self):
        self.plugins = set()

    """
    add a :class:`Plugin` to the :class:`Context`
    """

    def register(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.add(p)
        return self

    """
    remove a :class:`Plugin` from the :class:`Context`
    """

    def unregister(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.remove(p)
        return self

    def textfile(self, file_path: str) -> 'DataQuanta[str]':
        return DataQuanta(self, TextFileSource(file_path))

    def __str__(self):
        return "Plugins: {}".format(str(self.plugins))

    def __repr__(self):
        return self.__str__()


class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    context: WayangContext

    def __init__(self, context: WayangContext, operator: PywyOperator):
        self.operator = operator
        self.context = context

    def filter(self: "DataQuanta[T]", p: Predicate) -> "DataQuanta[T]":
        return DataQuanta(self.context, self._connect(FilterOperator(p)))

    def map(self: "DataQuanta[In]", f: Function) -> "DataQuanta[Out]":
        return DataQuanta(self.context, self._connect(MapOperator(f)))

    def flatmap(self: "DataQuanta[In]", f: FlatmapFunction) -> "DataQuanta[IterableOut]":
        return DataQuanta(self.context, self._connect(FlatmapOperator(f)))

    def store_textfile(self: "DataQuanta[In]", path: str, end_line: str = None):
        last: List[SinkOperator] = [
            cast(
                SinkOperator,
                self._connect(
                    TextFileSink(
                        path,
                        self.operator.outputSlot[0],
                        end_line
                    )
                )
            )
        ]
        PywyPlan(self.context.plugins, last).execute()

    def _connect(self, op: PO_T, port_op: int = 0) -> PywyOperator:
        self.operator.connect(0, op, port_op)
        return op

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
