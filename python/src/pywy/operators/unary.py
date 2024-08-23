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

from itertools import chain, groupby
from collections import defaultdict
import ast

from pywy.operators.base import PywyOperator
from pywy.types import (
                            GenericTco,
                            GenericUco,
                            Predicate,
                            get_type_predicate,
                            Function,
                            BiFunction,
                            get_type_function,
                            get_type_bifunction,
                            FlatmapFunction,
                            get_type_flatmap_function
                        )


class UnaryToUnaryOperator(PywyOperator):

    def __init__(self, name: str, input_type: GenericTco = None, output_type: GenericTco = None):
        super().__init__(name, "unary", input_type, output_type, 1, 1)

    def postfix(self) -> str:
        return 'OperatorUnary'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FilterOperator(UnaryToUnaryOperator):

    predicate: Predicate
    json_name: str

    def __init__(self, predicate: Predicate, input_type: GenericTco = None):
        if input_type is None:
            input_type = get_type_predicate(predicate) if predicate else None
        super().__init__("Filter", input_type, input_type)
        self.predicate = predicate
        self.json_name = "filter"

    def use_predicate(self, iterator) -> bool:
        return self.predicate(next(iterator))

    def get_udf(self, iterator):
        return filter(self.predicate, iterator)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class MapOperator(UnaryToUnaryOperator):

    function: Function
    json_name: str

    def __init__(self, function: Function, input_type: GenericTco = None, output_type: GenericTco = None):
        if input_type is None or output_type is None:
            input_type, output_type = get_type_function(function) if function else (None, None)
        super().__init__("Map", input_type, output_type)
        self.function = function
        self.json_name = "map"

    def get_udf(self, iterator):
        return map(lambda x: self.function(x), iterator)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class MapPartitionsOperator(UnaryToUnaryOperator):

    function: Function
    json_name: str

    def __init__(self, function: Function, input_type: GenericTco = None, output_type: GenericTco = None):
        if input_type is None or output_type is None:
            input_type, output_type = get_type_function(function) if function else (None, None)
        super().__init__("MapPartitions", input_type, output_type)
        self.function = function
        self.json_name = "mapPartitions"

    def get_udf(self, iterator):
        return map(lambda x: self.function(x), iterator)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FlatmapOperator(UnaryToUnaryOperator):

    fm_function: FlatmapFunction
    json_name: str


    def __init__(self, fm_function: FlatmapFunction, input_type: GenericTco = None, output_type: GenericTco = None):
        if input_type is None or output_type is None:
            input_type, output_type = get_type_flatmap_function(fm_function) if fm_function else (None, None)
        super().__init__("Flatmap", input_type, output_type)
        self.fm_function = fm_function
        self.json_name = "flatMap"

    def get_udf(self, iterator):
        return chain.from_iterable(map(lambda x: self.fm_function(x), iterator))

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class ReduceByKeyOperator(UnaryToUnaryOperator):
    key_function: Function
    reduce_function: BiFunction
    json_name: str

    def __init__(
            self,
            key_function: Function,
            reduce_function: BiFunction,
            input_type: GenericTco = None,
        ):
        if input_type is None:
            input_type = get_type_bifunction(reduce_function) if reduce_function else (None, None, None)
        super().__init__("ReduceByKey", (input_type[0], input_type[1]))
        self.key_function = key_function
        self.reduce_function = reduce_function
        self.json_name = "reduceBy"

    def get_udf(self, iterator):
        # Use ast.literal_eval() to safely evaluate the string as a Python literal
        #print(", ".join(iterator))
        #list_of_tuples = ast.literal_eval("[" + ", ".join(iterator)  + "]")

        tuples = [(str(item[0]), str(item[1])) for item in iterator]
        grouped_data = groupby(sorted(tuples, key=self.key_function), key=self.key_function)

        # Create a defaultdict to store the sums
        sums = {}

        for key, group in grouped_data:
            sums[key] = (key, 1)
            for item in group:
                sums[key] = self.reduce_function(sums[key], item)

        return sums.values()

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class SortOperator(UnaryToUnaryOperator):

    key_udf: Function
    json_name: str

    def __init__(self, function: Function, input_type: GenericTco = None):
        if input_type is None:
            input_type, output_type = get_type_function(function) if function else (None, None)
        super().__init__("Sort", input_type, None)
        self.key_udf = function
        self.json_name = "sort"

    def get_udf(self, iterator):
        return sorted(iterator, key=self.key_udf)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

