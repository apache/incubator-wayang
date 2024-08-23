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
from pywy.operators.unary import MapOperator
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator
from pywy.platforms.commons.channels import (
    COMMONS_CALLABLE_CHANNEL_DESCRIPTOR,
    CommonsCallableChannel
)
from pywy.platforms.python.channels import (
    PyIteratorChannel,
    PY_ITERATOR_CHANNEL_DESCRIPTOR,
)


class PyMapOperator(MapOperator, PyExecutionOperator):

    def __init__(self, origin: MapOperator = None, **kwargs):
        function = None if origin is None else origin.function
        super().__init__(function)
        pass

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        udf = self.function
        if isinstance(inputs[0], PyIteratorChannel):
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            py_out_iter_channel: PyIteratorChannel = outputs[0]
            py_out_iter_channel.accept_iterable(map(udf, py_in_iter_channel.provide_iterable()))
        elif isinstance(inputs[0], CommonsCallableChannel):
            py_in_call_channel: CommonsCallableChannel = inputs[0]
            py_out_call_channel: CommonsCallableChannel = outputs[0]

            def func(iterator):
                return map(udf, iterator)

            py_out_call_channel.accept_callable(
                CommonsCallableChannel.concatenate(
                    func,
                    py_in_call_channel.provide_callable()
                )
            )
        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR, COMMONS_CALLABLE_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR, COMMONS_CALLABLE_CHANNEL_DESCRIPTOR}
