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

from pywy.basic.model.models import Model
from pywy.basic.model.option import Option
from pywy.operators.base import PywyOperator
from pywy.types import (
    GenericTco,
    Function
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
    ):
        super().__init__("Join", input_type, (input_type, input_type))
        self.this_key_function = this_key_function
        self.that = that
        self.that_key_function = that_key_function
        self.json_name = "join"

    def get_left_key_udf(self, iterator):
        iterator = self.serialize_iterator(iterator)
        return map(lambda x: self.this_key_function(x), iterator)

    def get_right_key_udf(self, iterator):
        iterator = self.serialize_iterator(iterator)
        print("right")
        print(iterator)
        print(list(map(lambda x: self.that_key_function(x), iterator)))
        return map(lambda x: self.that_key_function(x), iterator)


class CartesianOperator(BinaryToUnaryOperator):
    that: PywyOperator
    json_name: str

    def __init__(
        self,
        that: PywyOperator,
        input_type: GenericTco,
    ):
        super().__init__("Cartesian", input_type, input_type)
        self.that = that
        self.json_name = "cartesian"


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
