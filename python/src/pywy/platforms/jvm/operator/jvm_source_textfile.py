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
from pywy.operators.source import TextFileSource
from pywy.platforms.jvm.channels import DISPATCHABLE_CHANNEL_DESCRIPTOR, DispatchableChannel
from pywy.platforms.jvm.operator.jvm_execution_operator import JVMExecutionOperator
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMTextFileSource, WayangJVMOperator


class JVMTextFileSourceOperator(TextFileSource, JVMExecutionOperator):

    def __init__(self, origin: TextFileSource = None, **kwargs):
        path = None if origin is None else origin.path
        super().__init__(path)
        self.set_context(**kwargs)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        if isinstance(outputs[0], DispatchableChannel):
            py_out_dispatch_channel: DispatchableChannel = outputs[0]
            py_out_dispatch_channel.accept_dispatchable(
                WayangJVMTextFileSource(self.name, self.path)
            )
        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The JVMTextFileSource does not support Input Channels")

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {DISPATCHABLE_CHANNEL_DESCRIPTOR}
