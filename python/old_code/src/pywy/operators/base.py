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

from typing import (TypeVar, Optional, List, Set)
from pywy.core import ChannelDescriptor
from pywy.core.channel import CH_T, CHD_T


class PywyOperator:

    inputSlot: List[TypeVar]
    inputChannel: List[CH_T]
    inputChannelDescriptor: List[CHD_T]
    inputOperator: List['PywyOperator']
    inputs: int
    outputSlot: List[TypeVar]
    outputChannel: List[CH_T]
    outputChannelDescriptor: List[CHD_T]
    outputOperator: List['PywyOperator']
    outputs: int

    def __init__(self,
                 name: str,
                 input_type: TypeVar = None,
                 output_type: TypeVar = None,
                 input_length: Optional[int] = 1,
                 output_length: Optional[int] = 1,
                 *args,
                 **kwargs
                 ):
        self.name = (self.prefix() + name + self.postfix()).strip()
        self.inputSlot = [input_type]
        self.inputs = input_length
        self.outputSlot = [output_type]
        self.outputs = output_length
        self.inputOperator = [None] * self.inputs
        self.outputOperator = [None] * self.outputs
        self.inputChannel = [None] * self.inputs
        self.outputChannel = [None] * self.outputs

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

    def validate_channels(self, inputs, outputs):
        self.validate_inputs(inputs)
        self.validate_outputs(outputs)

    def connect(self, port: int, that: 'PO_T', port_that: int):
        self.outputOperator[port] = that
        that.inputOperator[port_that] = self

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        pass

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        pass

    def prefix(self) -> str:
        return ''

    def postfix(self) -> str:
        return ''

    def name_basic(self, with_prefix: bool = False, with_postfix: bool = True):
        prefix = len(self.prefix()) if not with_prefix else 0
        postfix = len(self.postfix()) if not with_postfix else 0
        return self.name[prefix:len(self.name) - postfix]

    def __str__(self):
        return "BaseOperator: \n\t- name: {}\n\t- inputs: {} {}\n\t- outputs: {} {} \n".format(
            str(self.name),
            str(self.inputs),
            str(self.inputSlot),
            str(self.outputs),
            str(self.outputSlot),
        )

    def __repr__(self):
        return self.__str__()


PO_T = TypeVar('PO_T', bound=PywyOperator)
