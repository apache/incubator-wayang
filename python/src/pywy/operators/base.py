from typing import (TypeVar, Optional, List, Set)
from pywy.core import ChannelDescriptor, Channel


class PywyOperator:

    inputSlot: List[TypeVar]
    inputChannel: List[Channel]
    inputChannelDescriptor: List[ChannelDescriptor]
    inputOperator: List['PywyOperator']
    inputs: int
    outputSlot: List[TypeVar]
    outputChannel: List[Channel]
    outputChannelDescriptor: List[ChannelDescriptor]
    outputOperator: List['PywyOperator']
    outputs: int

    def __init__(self,
                 name: str,
                 input_type: TypeVar = None,
                 output_type: TypeVar = None,
                 input_length: Optional[int] = 1,
                 output_length: Optional[int] = 1
                 ):
        self.name = (self.prefix() + name + self.postfix()).strip()
        self.inputSlot = [input_type]
        self.inputs = input_length
        self.outputSlot = [output_type]
        self.outputs = output_length
        self.inputOperator = [None] * self.inputs
        self.outputOperator = [None] * self.outputs
        self.inputChannel = [None] * self.inputs
        self.outputChannel = [None] * self.outputs

    def validate_inputs(self, vec):
        if len(vec) != self.inputs:
            raise Exception(
                "the inputs channel contains {} elements and need to have {}".format(
                    len(vec),
                    self.inputs
                )
            )

    def validate_outputs(self, vec):
        if len(vec) != self.outputs:
            raise Exception(
                "the output channel contains {} elements and need to have {}".format(
                    len(vec),
                    self.inputs
                )
            )

    def validate_channels(self, inputs, outputs):
        self.validate_inputs(inputs)
        self.validate_outputs(outputs)

    def connect(self, port: int, that: 'PywyOperator', port_that: int):
        self.outputOperator[port] = that
        that.inputOperator[port_that] = self

    def get_input_channeldescriptors(self) -> Set[ChannelDescriptor]:
        pass

    def get_output_channeldescriptors(self) -> Set[ChannelDescriptor]:
        pass

    def prefix(self) -> str:
        return ''

    def postfix(self) -> str:
        return ''

    def name_basic(self, with_prefix: bool = False, with_postfix: bool = True):
        prefix = len(self.prefix()) if not with_prefix else 0
        postfix = len(self.postfix()) if not with_postfix else 0
        return self.name[prefix:len(self.name) - postfix]

    def __str__(self):
        return "BaseOperator: \n\t- name: {}\n\t- inputs: {} {}\n\t- outputs: {} {} \n".format(
            str(self.name),
            str(self.inputs),
            str(self.inputSlot),
            str(self.outputs),
            str(self.outputSlot),
        )

    def __repr__(self):
        return self.__str__()
