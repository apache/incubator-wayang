from pywy.wayangplan.base import PywyOperator

class SourceUnaryOperator(PywyOperator):

    def __init__(self, name:str):
        super(SourceUnaryOperator, self).__init__(
            name = name,
            input = None,
            output = str,
            input_lenght = 0,
            output_lenght = 1
        )

    def postfix(self) -> str:
        return 'Source'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class TextFileSource(SourceUnaryOperator):

    path: str

    def __init__(self, path: str):
        super(TextFileSource, self).__init__('TextFile')
        self.path = path

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()