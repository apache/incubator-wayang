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

from typing import ( Iterable, Callable )
from pywy.core import (Channel, ChannelDescriptor)


class PyIteratorChannel(Channel):

    iterable: Iterable

    def __init__(self):
        Channel.__init__(self)

    def provide_iterable(self) -> Iterable:
        return self.iterable

    def accept_iterable(self, iterable: Iterable) -> 'PyIteratorChannel':
        self.iterable = iterable
        return self


class PyCallableChannel(Channel):

    udf: Callable

    def __init__(self):
        Channel.__init__(self)

    def provide_callable(self) -> Callable:
        return self.udf

    def accept_callable(self, udf: Callable) -> 'PyCallableChannel':
        self.udf = udf
        return self

    @staticmethod
    def concatenate(function_a: Callable, function_b: Callable):
        def executable(iterable):
            return function_a(function_b(iterable))
        return executable


class PyFileChannel(Channel):

    path: str

    def __init__(self):
        Channel.__init__(self)

    def provide_path(self) -> str:
        return self.path

    def accept_path(self, path: str) -> 'PyIteratorChannel':
        self.path = path
        return self


PY_ITERATOR_CHANNEL_DESCRIPTOR = ChannelDescriptor(type(PyIteratorChannel()), False, False)
PY_CALLABLE_CHANNEL_DESCRIPTOR = ChannelDescriptor(type(PyCallableChannel()), False, False)
PY_FILE_CHANNEL_DESCRIPTOR = ChannelDescriptor(type(PyFileChannel()), False, False)
