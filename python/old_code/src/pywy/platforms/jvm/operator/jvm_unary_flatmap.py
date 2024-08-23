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
from itertools import chain

from pywy.core.channel import CH_T, ChannelDescriptor
from pywy.operators.unary import FlatmapOperator
from pywy.platforms.jvm.channels import DISPATCHABLE_CHANNEL_DESCRIPTOR, DispatchableChannel
from pywy.platforms.jvm.operator.jvm_execution_operator import JVMExecutionOperator
from pywy.platforms.commons.channels import (
    CommonsCallableChannel
)
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMMappartitionOperator, WayangJVMOperator


class JVMFlatmapOperator(FlatmapOperator, JVMExecutionOperator):

    def __init__(self, origin: FlatmapOperator = None, **kwargs):
        fm_function = None if origin is None else origin.fm_function
        super().__init__(fm_function)
        self.set_context(**kwargs)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        udf = self.fm_function
        if isinstance(inputs[0], DispatchableChannel):
            py_in_dispatch_channel: DispatchableChannel = inputs[0]
            py_out_dispatch_channel: DispatchableChannel = outputs[0]

            def func(iterator):
                return chain.from_iterable(map(udf, iterator))

            py_out_dispatch_channel.accept_callable(
                CommonsCallableChannel.concatenate(
                    func,
                    py_in_dispatch_channel.provide_callable()
                )
            )

            op: WayangJVMOperator = py_in_dispatch_channel.provide_dispatchable()

            if isinstance(op, WayangJVMMappartitionOperator):
                py_out_dispatch_channel.accept_dispatchable(op)
                return

            current: WayangJVMMappartitionOperator = WayangJVMMappartitionOperator(self.name)
            # TODO check for the case where the index matter
            op.connect_to(0, current, 0)
            self.close_operator(op)
            py_out_dispatch_channel.accept_dispatchable(current)

        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {DISPATCHABLE_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {DISPATCHABLE_CHANNEL_DESCRIPTOR}
