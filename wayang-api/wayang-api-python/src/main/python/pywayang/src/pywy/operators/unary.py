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
                            FlatmapFunction,
                            get_type_flatmap_function
                        )


class UnaryToUnaryOperator(PywyOperator):

    def __init__(self, name: str):
        super().__init__(name, "unary", 1, 1)

    def postfix(self) -> str:
        return 'OperatorUnary'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FilterOperator(UnaryToUnaryOperator):

    predicate: Predicate
    json_name: str

    def __init__(self, predicate: Predicate):
        super().__init__("Filter")
        self.predicate = predicate
        self.json_name = "filter"

    def get_udf(self, iterator):
        return filter(self.predicate, iterator)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class MapOperator(UnaryToUnaryOperator):

    function: Function
    json_name: str

    def __init__(self, function: Function):
        super().__init__("Map")
        self.function = function
        self.json_name = "map"

    def get_udf(self, iterator):
        return map(self.function, iterator)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FlatmapOperator(UnaryToUnaryOperator):

    fm_function: FlatmapFunction
    json_name: str

    def __init__(self, fm_function: FlatmapFunction):
        super().__init__("Flatmap")
        self.fm_function = fm_function
        self.json_name = "flatMap"

    def get_udf(self, iterator):
        return chain.from_iterable(map(self.fm_function, iterator))

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class ReduceByKeyOperator(UnaryToUnaryOperator):
    reduce_function: BiFunction
    json_name: str

    def __init__(self, reduce_function: BiFunction):
        super().__init__("ReduceByKey")
        self.reduce_function = reduce_function
        self.json_name = "reduceBy"

    def get_udf(self, iterator):
        # Use ast.literal_eval() to safely evaluate the string as a Python literal
        list_of_tuples = ast.literal_eval("[" + ", ".join(iterator)  + "]")

        tuples = [(str(item[0]), str(item[1])) for item in list_of_tuples]
        print(list(tuples))
        grouped_data = groupby(sorted(tuples, key=lambda x: x[0]), key=lambda x: x[0])

        # Create a defaultdict to store the sums
        sums = {}

        for key, group in grouped_data:
            sums[key] = (key, 1)
            print(sums[key])
            for item in group:
                sums[key] = self.reduce_function(sums[key], item)

        return sums.values()

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
