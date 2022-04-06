from pywayang.operator.unary import FilterOperator
from pywayang.platforms.python.operators.PythonExecutionOperator import PythonExecutionOperator
from pywayang.platforms.python.channels import (Channel, ChannelDescriptor, PyIteratorChannel,
                                                PyIteratorChannelDescriptor, PyCallableChannelDescriptor,
                                                PyCallableChannel)
from typing import Set

class PyFilterOperator(FilterOperator, PythonExecutionOperator):

    def __init__(self, origin: FilterOperator = None):
        predicate = None if origin is None else origin.predicate
        super().__init__(predicate)
        pass

    def execute(self, inputs: Channel, outputs: Channel):
        self.validateChannels(inputs, outputs)
        udf = self.predicate
        if isinstance(inputs[0], PyIteratorChannel) :
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            py_out_iter_channel: PyIteratorChannel = outputs[0]
            py_out_iter_channel.accept_iterable(filter(udf, py_in_iter_channel.provide_iterable()))
        elif isinstance(inputs[0], PyCallableChannel) :
            py_in_call_channel: PyCallableChannel = inputs[0]
            py_out_call_channel: PyCallableChannel = outputs[0]

            def func(iterator):
                return filter(udf, iterator)

            py_out_call_channel.accept_callable(
                PyCallableChannel.concatenate(
                    func,
                    py_in_call_channel.provide_callable()
                )
            )
        else:
            raise Exception("Channel Type does not supported")


    def getInputChannelDescriptors(self) -> Set[ChannelDescriptor]:
        return {PyIteratorChannelDescriptor, PyCallableChannelDescriptor}

    def getOutputChannelDescriptors(self) -> Set[ChannelDescriptor]:
        return {PyIteratorChannelDescriptor, PyCallableChannelDescriptor}
