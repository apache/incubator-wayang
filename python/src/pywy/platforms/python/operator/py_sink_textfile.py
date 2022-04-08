from typing import Set, List, Type

from pywy.core.channel import CH_T
from pywy.operators.sink import TextFileSink
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator
from pywy.platforms.python.channels import (
    ChannelDescriptor,
    PyIteratorChannel,
    PY_ITERATOR_CHANNEL_DESCRIPTOR
)


class PyTextFileSinkOperator(TextFileSink, PyExecutionOperator):

    def __init__(self, origin: TextFileSink = None):
        path = None if origin is None else origin.path
        type_class = None if origin is None else origin.inputSlot[0]
        super().__init__(path, type_class)

    def execute(self, inputs: List[Type[CH_T]], outputs: List[Type[CH_T]]):
        self.validate_channels(inputs, outputs)
        if isinstance(inputs[0], PyIteratorChannel):
            file = open(self.path, 'w')
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            iterable = py_in_iter_channel.provide_iterable()
            if self.inputSlot[0] == str:
                for element in iterable:
                    file.write(element)
            else:
                for element in iterable:
                    file.write("{}\n".format(str(element)))
            file.close()

        else:
            raise Exception("Channel Type does not supported")

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PY_ITERATOR_CHANNEL_DESCRIPTOR}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The PyTextFileSource does not support Output Channels")
