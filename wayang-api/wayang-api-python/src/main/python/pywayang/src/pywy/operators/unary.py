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

from pywy.operators.base import PywyOperator
from pywy.types import (
                            GenericTco,
                            GenericUco,
                            Predicate,
                            get_type_predicate,
                            Function,
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

    def __init__(self, predicate: Predicate):
        super().__init__("Filter")
        self.predicate = predicate

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class MapOperator(UnaryToUnaryOperator):

    function: Function

    def __init__(self, function: Function):
        super().__init__("Map")
        self.function = function

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FlatmapOperator(UnaryToUnaryOperator):

    fm_function: FlatmapFunction

    def __init__(self, fm_function: FlatmapFunction):
        super().__init__("Flatmap")
        self.fm_function = fm_function

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
