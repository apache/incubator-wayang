from pywayang.operator.source import TextFileSource
from pywayang.platforms.python.operators.PythonExecutionOperator import PythonExecutionOperator
from pywayang.platforms.python.channels import (
                                                    Channel,
                                                    ChannelDescriptor,
                                                    PyIteratorChannel,
                                                    PyIteratorChannelDescriptor,
                                                    PyFileChannelDescriptor
                                                )
from typing import Set

class PyTextFileSourceOperator(TextFileSource, PythonExecutionOperator):

    def __init__(self, origin: TextFileSource = None):
        path = None if origin is None else origin.path
        super().__init__(path)
        pass

    def execute(self, inputs: Channel, outputs: Channel):
        self.validateChannels(inputs, outputs)
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


    def getInputChannelDescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The PyTextFileSource does not support Input Channels")

    def getOutputChannelDescriptors(self) -> Set[ChannelDescriptor]:
        return {PyIteratorChannelDescriptor}
