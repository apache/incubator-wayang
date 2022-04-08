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

from typing import TypeVar


class Channel:

    def __init__(self):
        pass

    def get_channel(self) -> 'Channel':
        return self

    def get_type(self):
        return type(self)


class ChannelDescriptor:

    def __init__(self, channel_type: type, is_reusable: bool, is_suitable_for_breakpoint: bool):
        self.channelType = channel_type
        self.isReusable = is_reusable
        self.isSuitableForBreakpoint = is_suitable_for_breakpoint

    def create_instance(self) -> Channel:
        return self.channelType()


CH_T = TypeVar('CH_T', bound=Channel)
CHD_T = TypeVar('CHD_T', bound=ChannelDescriptor)
