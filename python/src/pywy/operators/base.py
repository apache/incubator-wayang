from typing import ( TypeVar, Optional, List, Set )
from pywy.core import ChannelDescriptor, Channel

class PywyOperator:

    inputSlot : List[TypeVar]
    inputChannel : List[Channel]
    inputChannelDescriptor : List[ChannelDescriptor]
    inputOperator: List['PywyOperator']
    inputs : int
    outputSlot : List[TypeVar]
    outputChannel: List[Channel]
    outputChannelDescriptor: List[ChannelDescriptor]
    outputOperator: List['PywyOperator']
    outputs: int

    def __init__(self,
                 name: str,
                 input: Optional[TypeVar] = None,
                 output: Optional[TypeVar] = None,
                 input_lenght: Optional[int] = 1,
                 output_lenght: Optional[int] = 1
    ):
        self.name = (self.prefix() + name + self.postfix()).strip()
        self.inputSlot = input
        self.inputs = input_lenght
        self.outputSlot = output
        self.outputs = output_lenght
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
    def validate_channels(self, input, output):
        self.validate_inputs(input)
        self.validate_outputs(output)

    def connect(self, port:int, that: 'PywyOperator', port_that:int):
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

    def name_basic(self, with_prefix: bool = False, with_postfix:bool = True):
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



