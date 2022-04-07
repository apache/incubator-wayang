from typing import Set
from pywy.operators.source import TextFileSource
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator
from pywy.platforms.python.channels import (
                                                Channel,
                                                ChannelDescriptor,
                                                PyIteratorChannel,
                                                PyIteratorChannelDescriptor
                                            )


class PyTextFileSourceOperator(TextFileSource, PyExecutionOperator):

    def __init__(self, origin: TextFileSource = None):
        path = None if origin is None else origin.path
        super().__init__(path)
        pass

    def execute(self, inputs: Channel, outputs: Channel):
        self.validate_channels(inputs, outputs)
        if isinstance(outputs[0], PyIteratorChannel) :
            py_out_iter_channel: PyIteratorChannel = outputs[0]
            py_out_iter_channel.accept_iterable(
                open(
                    self.path,
                    'r'
                )
            )

        else:
            raise Exception("Channel Type does not supported")


    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The PyTextFileSource does not support Input Channels")

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PyIteratorChannelDescriptor}
