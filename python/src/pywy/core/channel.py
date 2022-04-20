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
    """ Models the data movement between to executables :py:class:`pywy.operators.base.PywyOperator`

    Channel is the structure that is used to move the data between executables
    :py:class:`pywy.operators.base.PywyOperator` and it helps to identify the
    compatibility between the interaction of operators
    """

    def __init__(self):
        pass

    def get_type(self):
        """ return the type of the channel

        Returns
        -------
        :py:class:`typing.Type` of the current Channel
        """
        return type(self)


class ChannelDescriptor:
    """

    Attributes
    ----------
    channel_type : :py:class:`typing.Type`
       Type of the :py:class:`pywy.core.Channel` that the descriptor
       will generate
    is_reusable : bool
        indicates if the source for the channel is reusable for several consumer
    is_suitable_for_breakpoint : bool
        indicates if the element support the breakpoint strategies
    """

    def __init__(self, channel_type: type, is_reusable: bool, is_suitable_for_breakpoint: bool):
        """Basic constructor of the ChannelDescriptor

        Parameters
        ----------
        channel_type
            Description of `channelType`.
        is_reusable
            Description of `is_reusable`.
        is_suitable_for_breakpoint
            Description of `is_suitable_for_breakpoint`.
        """
        self.channel_type = channel_type
        self.is_reusable = is_reusable
        self.is_suitable_for_breakpoint = is_suitable_for_breakpoint

    def create_instance(self) -> Channel:
        """Generates an instance of :py:class:`pywy.core.Channel`

        Returns
        -------
            instance of :py:class:`pywy.core.Channel`
        """
        return self.channel_type()


CH_T = TypeVar('CH_T', bound=Channel)
CHD_T = TypeVar('CHD_T', bound=ChannelDescriptor)
