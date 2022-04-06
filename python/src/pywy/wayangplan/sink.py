from typing import Any

from pywy.types import GenericTco
from pywy.wayangplan.base import WyOperator

class SinkOperator(WyOperator):

    def postfix(self) -> str:
        return 'Sink'

class SinkUnaryOperator(SinkOperator):

    def __init__(self, name:str, input:GenericTco=Any):
        super().__init__(name, input, None, 1, 0)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class TextFileSink(SinkUnaryOperator):

    path: str

    def __init__(self, path: str):
        super().__init__('TextFile')
        self.path = path

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()