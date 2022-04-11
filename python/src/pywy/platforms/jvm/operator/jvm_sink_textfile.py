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
from pywy.exception import PywyException
from pywy.operators.sink import TextFileSink
from pywy.platforms.jvm.channels import DISPATCHABLE_CHANNEL_DESCRIPTOR, DispatchableChannel
from pywy.platforms.jvm.operator.jvm_execution_operator import JVMExecutionOperator
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMTextFileSink


class JVMTextFileSinkOperator(TextFileSink, JVMExecutionOperator):

    def __init__(self, origin: TextFileSink = None, **kwargs):
        path = None if origin is None else origin.path
        type_class = None if origin is None else origin.inputSlot[0]
        end_line = None if origin is None else origin.end_line
        super().__init__(path, type_class, end_line)
        self.set_context(**kwargs)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)

        if isinstance(inputs[0], DispatchableChannel):

            py_in_dispatch_channel: DispatchableChannel = inputs[0]
            operator = py_in_dispatch_channel.provide_dispatchable(do_wrapper=True)

            sink: WayangJVMTextFileSink = WayangJVMTextFileSink(self.name, self.path)

            operator.connect_to(0, sink, 0)

            self.close_operator(operator)
            self.close_operator(sink)

            self.dispatch_operator = sink
        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {DISPATCHABLE_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The JVMTextFileSink does not support Output Channels")
