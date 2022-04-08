from typing import Any

from pywy.types import GenericTco
from pywy.operators.base import PywyOperator


class SinkOperator(PywyOperator):

    def postfix(self) -> str:
        return 'Sink'


class SinkUnaryOperator(SinkOperator):

    def __init__(self, name: str, input_type: GenericTco = Any):
        super().__init__(name, input_type, None, 1, 0)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class TextFileSink(SinkUnaryOperator):

    path: str

    def __init__(self, path: str, input_type: GenericTco):
        super().__init__('TextFile', input_type)
        self.path = path

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
