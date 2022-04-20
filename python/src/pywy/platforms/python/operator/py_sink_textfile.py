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

from typing import Set, List, Type

from pywy.core.channel import (CH_T, ChannelDescriptor)
from pywy.operators.sink import TextFileSink
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator
from pywy.platforms.python.channels import (
    PyIteratorChannel,
    PY_ITERATOR_CHANNEL_DESCRIPTOR
)


class PyTextFileSinkOperator(TextFileSink, PyExecutionOperator):

    def __init__(self, origin: TextFileSink = None, **kwargs):
        path = None if origin is None else origin.path
        type_class = None if origin is None else origin.inputSlot[0]
        end_line = None if origin is None else origin.end_line
        super().__init__(path, type_class, end_line)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        if isinstance(inputs[0], PyIteratorChannel):
            file = open(self.path, 'w')
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            iterable = py_in_iter_channel.provide_iterable()

            if self.inputSlot[0] == str and self.end_line is None:
                for element in iterable:
                    file.write(element)
            else:
                for element in iterable:
                    file.write("{}{}".format(str(element), self.end_line))
            file.close()

        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The PyTextFileSource does not support Output Channels")
