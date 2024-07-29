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

from typing import Dict, Set, List, cast

from pywy.core.core import Plugin, PywyPlan
from pywy.operators.base import PO_T
from pywy.types import (GenericTco, Predicate, Function, BiFunction, FlatmapFunction, IterableOut, T, In, Out)
from pywy.operators import *
from pywy.basic.model.ops import Op
from pywy.basic.model.option import Option
from pywy.basic.model.models import Model


class Configuration:
    entries: Dict[str, str]

    def __init__(self):
        self.entries = {}

    def set_property(self, key: str, value: str):
        self.entries[key] = value


class WayangContext:
    """
    This is the entry point for users to work with Wayang.
    """
    plugins: Set[Plugin]
    configuration: Configuration

    def __init__(self, configuration: Configuration = Configuration()):
        self.plugins = set()
        self.configuration = configuration

    """
    add a :class:`Plugin` to the :class:`Context`
    """

    def register(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.update(p)
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

    def filter(self: "DataQuanta[T]", p: Predicate, input_type: GenericTco = None) -> "DataQuanta[T]":
        return DataQuanta(self.context, self._connect(FilterOperator(p, input_type)))

    def map(self: "DataQuanta[In]", f: Function, input_type: GenericTco = None, output_type: GenericTco = None) -> "DataQuanta[Out]":
        return DataQuanta(self.context, self._connect(MapOperator(f, input_type, output_type)))

    def flatmap(self: "DataQuanta[In]", f: FlatmapFunction, input_type: GenericTco = None, output_type: GenericTco = None) -> "DataQuanta[IterableOut]":
        return DataQuanta(self.context, self._connect(FlatmapOperator(f, input_type, output_type)))

    def reduce_by_key(self: "DataQuanta[In]",
                      key_f: Function,
                      f: BiFunction,
                      input_type: GenericTco = None
                      ) -> "DataQuanta[IterableOut]":

        return DataQuanta(self.context, self._connect(ReduceByKeyOperator(key_f, f, input_type)))

    def sort(self: "DataQuanta[In]",
                      key_f: Function,
                      input_type: GenericTco = None
                      ) -> "DataQuanta[IterableOut]":

        return DataQuanta(self.context, self._connect(SortOperator(key_f, input_type)))

    def join(
        self: "DataQuanta[In]",
        this_key_f: Function,
        that: "DataQuanta[In]",
        that_key_f: Function,
        input_type: GenericTco = None,
        output_type: GenericTco = None
        ) -> "DataQuanta[Out]":

        op = JoinOperator(
            this_key_f,
            that,
            that_key_f,
            input_type,
            output_type
        )

        self._connect(op),
        return DataQuanta(
            self.context,
            that._connect(op,1)
        )

    def dlTraining(
        self: "DataQuanta[In]",
        model: Model,
        option: Option,
        that: "DataQuanta[In]",
        input_type: GenericTco,
        output_type: GenericTco
    ) -> "DataQuanta[Out]":

        op = DLTrainingOperator(
            model,
            option,
            input_type,
            output_type
        )
        self._connect(op)

        return DataQuanta(
            self.context,
            that._connect(op,1)
        )

    def predict(
        self: "DataQuanta[In]",
        that: "DataQuanta[In]",
        input_type: GenericTco,
        output_type: GenericTco
    ) -> "DataQuanta[Out]":
        op = PredictOperator(
            input_type,
            output_type
        )

        self._connect(op)

        return DataQuanta(
            self.context,
            that._connect(op,1)
        )

    def store_textfile(self: "DataQuanta[In]", path: str, input_type: GenericTco = None):
        last: List[SinkOperator] = [
            cast(
                SinkOperator,
                self._connect(
                    TextFileSink(
                        path,
                        input_type
                    )
                )
            )
        ]
        #print(PywyPlan(self.context.plugins, last))
        PywyPlan(self.context.plugins, self.context.configuration.entries, last).execute()

    def _connect(self, op: PO_T, port_op: int = 0) -> PywyOperator:
        self.operator.connect(0, op, port_op)
        return op

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
