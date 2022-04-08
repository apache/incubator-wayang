from itertools import chain
from typing import Set, List, Type

from pywy.core.channel import CH_T
from pywy.operators.unary import FlatmapOperator
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator
from pywy.platforms.python.channels import (
                                                ChannelDescriptor,
                                                PyIteratorChannel,
                                                PY_ITERATOR_CHANNEL_DESCRIPTOR,
                                                PY_CALLABLE_CHANNEL_DESCRIPTOR,
                                                PyCallableChannel
                                            )


class PyFlatmapOperator(FlatmapOperator, PyExecutionOperator):

    def __init__(self, origin: FlatmapOperator = None):
        fm_function = None if origin is None else origin.fm_function
        super().__init__(fm_function)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        udf = self.fm_function
        if isinstance(inputs[0], PyIteratorChannel):
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            py_out_iter_channel: PyIteratorChannel = outputs[0]
            py_out_iter_channel.accept_iterable(chain.from_iterable(map(udf, py_in_iter_channel.provide_iterable())))
        elif isinstance(inputs[0], PyCallableChannel):
            py_in_call_channel: PyCallableChannel = inputs[0]
            py_out_call_channel: PyCallableChannel = outputs[0]

            def fm_func(iterator):
                return chain.from_iterable(map(udf, iterator))

            py_out_call_channel.accept_callable(
                PyCallableChannel.concatenate(
                    fm_func,
                    py_in_call_channel.provide_callable()
                )
            )
        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR, PY_CALLABLE_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR, PY_CALLABLE_CHANNEL_DESCRIPTOR}
