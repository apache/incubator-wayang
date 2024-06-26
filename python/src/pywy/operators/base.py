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

from typing import (TypeVar, Optional, List)
from pywy.types import (typecheck, ConstrainedOperatorType)


class PywyOperator:
    cat: str
    inputs: int
    outputs: int
    inputOperator: List['PywyOperator']
    outputOperator: List['PywyOperator']
    input_type: ConstrainedOperatorType
    output_type: ConstrainedOperatorType

    def __init__(self,
                 name: str,
                 cat: str,
                 input_type: ConstrainedOperatorType = None,
                 output_type: ConstrainedOperatorType = None,
                 input_length: Optional[int] = 1,
                 output_length: Optional[int] = 1,
                 *args,
                 **kwargs
                 ):
        typecheck(input_type)
        typecheck(output_type)
        self.name = (self.prefix() + name + self.postfix()).strip()
        self.cat = cat
        self.inputs = input_length
        self.outputs = output_length
        self.inputOperator = [None] * self.inputs
        self.outputOperator = [None] * self.outputs
        self.input_type = input_type
        self.output_type = output_type

    def validate_inputs(self, vec):
        if len(vec) != self.inputs:
            raise Exception(
                "the inputs channel contains {} elements and need to have {}".format(
                    len(vec),
                    self.inputs
                )
            )

    def validate_outputs(self, vec):
        if len(vec) != self.outputs:
            raise Exception(
                "the output channel contains {} elements and need to have {}".format(
                    len(vec),
                    self.inputs
                )
            )

    def connect(self, port: int, that: 'PO_T', port_that: int):
        self.outputOperator[port] = that
        that.inputOperator[port_that] = self

    def prefix(self) -> str:
        return ''

    def postfix(self) -> str:
        return ''

    def name_basic(self, with_prefix: bool = False, with_postfix: bool = True):
        prefix = len(self.prefix()) if not with_prefix else 0
        postfix = len(self.postfix()) if not with_postfix else 0
        return self.name[prefix:len(self.name) - postfix]

    def __str__(self):
        return "BaseOperator: \n\t- name: {}\n\t- inputs: {}\n\t- outputs: {}\n".format(
            str(self.name),
            str(self.inputs),
            str(self.outputs),
        )

    def __repr__(self):
        return self.__str__()

PO_T = TypeVar('PO_T', bound=PywyOperator)
