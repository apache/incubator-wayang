from typing import (TypeVar, Optional, List)


class BaseOperator:

    inputSlot : List[TypeVar]
    inputs : int
    outputSlot : List[TypeVar]
    outputs: int

    def __init__(self,
                 name: str,
                 input: Optional[TypeVar] = None,
                 output: Optional[TypeVar] = None,
                 input_lenght: Optional[int] = 1,
                 output_lenght: Optional[int] = 1
    ):
        self.name = name
        self.inputSlot = input
        self.inputs = input_lenght
        self.outputSlot = output
        self.outputs = output_lenght

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



