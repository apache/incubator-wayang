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
from typing import Callable

from pywy.core import (Channel, ChannelDescriptor)
from pywy.exception import PywyException
from pywy.platforms.commons.channels import CommonsCallableChannel
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMOperator, WayangJVMMappartitionOperator


class DispatchableChannel(CommonsCallableChannel):

    operator: WayangJVMOperator

    def __init__(self):
        Channel.__init__(self)
        self.udf = None
        self.operator = None

    def provide_dispatchable(self, do_wrapper: bool = False) -> WayangJVMOperator:
        if self.operator is None:
            raise PywyException("The operator was not define")
        if do_wrapper:
            self.operator.udf = self.udf

        return self.operator

    def accept_dispatchable(self, operator: WayangJVMOperator) -> 'WayangJVMOperator':
        self.operator = operator
        return self


DISPATCHABLE_CHANNEL_DESCRIPTOR = ChannelDescriptor(type(DispatchableChannel()), True, True)
