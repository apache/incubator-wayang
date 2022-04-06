from pywy.wayangplan.base import WyOperator

class SourceUnaryOperator(WyOperator):

    def __init__(self, name:str):
        super(SourceUnaryOperator, self).__init__(
            name = name,
            input = None,
            output = str,
            input_lenght = 0,
            output_lenght = 1
        )

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class TextFileSource(SourceUnaryOperator):

    path: str

    def __init__(self, path: str):
        super(TextFileSource, self).__init__('TextFileSource')
        self.path = path

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()