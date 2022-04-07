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
