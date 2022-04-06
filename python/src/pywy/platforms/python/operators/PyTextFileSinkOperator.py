from typing import Set
from pywy.wayangplan.sink import TextFileSink
from pywy.platforms.python.operators.PythonExecutionOperator import PythonExecutionOperator
from pywy.platforms.python.channels import (
                                                Channel,
                                                ChannelDescriptor,
                                                PyIteratorChannel,
                                                PyIteratorChannelDescriptor
                                            )


class PyTextFileSinkOperator(TextFileSink, PythonExecutionOperator):

    def __init__(self, origin: TextFileSink = None):
        path = None if origin is None else origin.path
        super().__init__(path)
        pass

    def execute(self, inputs: Channel, outputs: Channel):
        self.validate_channels(inputs, outputs)
        if isinstance(inputs[0], PyIteratorChannel) :
            file = open(self.path,'w')
            py_in_iter_channel: PyIteratorChannel = inputs[0]
            iterable = py_in_iter_channel.provide_iterable();
            for element in iterable:
                file.write(str(element))
            file.close()

        else:
            raise Exception("Channel Type does not supported")


    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        return {PyIteratorChannelDescriptor}

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        raise Exception("The PyTextFileSource does not support Output Channels")
