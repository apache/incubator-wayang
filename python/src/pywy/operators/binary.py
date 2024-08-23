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
from pywy.basic.model.models import Model
from pywy.basic.model.option import Option
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

class BinaryToUnaryOperator(PywyOperator):

    def __init__(self, name: str, input_type: GenericTco = None, output_type: GenericTco = None):
        super().__init__(name, "binary", input_type, output_type, 2, 1)

    def postfix(self) -> str:
        return 'OperatorBinary'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class JoinOperator(BinaryToUnaryOperator):
    this_key_function: Function
    that: PywyOperator
    that_key_function: Function
    json_name: str

    def __init__(
            self,
            this_key_function: Function,
            that: PywyOperator,
            that_key_function: Function,
            input_type: GenericTco,
            output_type: GenericTco
        ):
        super().__init__("Join", input_type, output_type)
        self.this_key_function = lambda g: this_key_function(next(g))
        self.that = that
        self.that_key_function = lambda g: that_key_function(next(g))
        self.json_name = "join"


class DLTrainingOperator(BinaryToUnaryOperator):
    model: Model
    option: Option
    json_name: str

    def __init__(self, model: Model, option: Option, x_type: GenericTco, y_type: GenericTco):
        super().__init__("DLTraining", x_type, y_type)
        self.model = model
        self.option = option
        self.json_name = "dlTraining"


class PredictOperator(BinaryToUnaryOperator):
    json_name: str

    def __init__(self, input_type: GenericTco, output_type: GenericTco):
        super().__init__("Predict", input_type, output_type)
        self.json_name = "predict"
