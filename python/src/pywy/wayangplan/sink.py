from pywy.wayangplan.base import WyOperator

class SinkOperator(WyOperator):

    def __init__(self, name:str):
        super().__init__(name, None, str, 0, 1)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class SinkUnaryOperator(SinkOperator):

    def __init__(self, name:str):
        super().__init__(name, None, str, 0, 1)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class TextFileSink(SinkUnaryOperator):

    path: str

    def __init__(self, path: str):
        super().__init__('TextFileSink')
        self.path = path

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()