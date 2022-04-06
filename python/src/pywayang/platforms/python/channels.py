from typing import ( Iterable, Callable )

class Channel:

    def __init__(self):
        pass

    def getchannel(self) -> 'Channel':
        return self

    def gettype(self):
        return type(self)

class ChannelDescriptor:

    def __init__(self, channelType: type, isReusable: bool, isSuitableForBreakpoint: bool):
        self.channelType = channelType
        self.isReusable = isReusable
        self.isSuitableForBreakpoint = isSuitableForBreakpoint

    def create_instance(self) -> Channel:
        return self.channelType()


class PyIteratorChannel(Channel):

    iterable : Iterable

    def __init__(self):
        Channel.__init__(self)

    def provide_iterable(self) -> Iterable:
        return self.iterable

    def accept_iterable(self, iterable: Iterable) -> 'PyIteratorChannel':
        self.iterable = iterable
        return self

class PyCallableChannel(Channel):

    udf : Callable

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

    path : str

    def __init__(self):
        Channel.__init__(self)

    def provide_path(self) -> str:
        return self.path

    def accept_path(self, path: str) -> 'PyIteratorChannel':
        self.path = path
        return self

PyIteratorChannelDescriptor = ChannelDescriptor(type(PyIteratorChannel()), False, False)
PyCallableChannelDescriptor = ChannelDescriptor(type(PyCallableChannel()), False, False)
PyFileChannelDescriptor = ChannelDescriptor(type(PyFileChannel()), False, False)