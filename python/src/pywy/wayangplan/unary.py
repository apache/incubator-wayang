from pywy.wayangplan.base import WyOperator
from pywy.types import (
                                GenericTco,
                                GenericUco,
                                Predicate,
                                getTypePredicate,
                                Function,
                                getTypeFunction,
                                FlatmapFunction,
                                getTypeFlatmapFunction
                            )
from itertools import chain


class UnaryToUnaryOperator(WyOperator):

    def __init__(self, name:str, input:GenericTco, output:GenericUco):
        super().__init__(name, input, output, 1, 1)

    def postfix(self) -> str:
        return 'OperatorUnary'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class FilterOperator(UnaryToUnaryOperator):

    predicate: Predicate

    def __init__(self, predicate: Predicate):
        type = getTypePredicate(predicate) if predicate else None
        super().__init__("Filter", type, type)
        self.predicate = predicate

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class MapOperator(UnaryToUnaryOperator):

    function: Function

    def __init__(self, function: Function):
        types = getTypeFunction(function) if function else (None, None)
        super().__init__("Map", types[0], types[1])
        self.function = function

    def getWrapper(self):
        udf = self.function
        def func(iterator):
            return map(udf, iterator)
        return func

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FlatmapOperator(UnaryToUnaryOperator):

    fmfunction: FlatmapFunction

    def __init__(self, fmfunction: FlatmapFunction):
        types = getTypeFlatmapFunction(fmfunction) if fmfunction else (None, None)
        super().__init__("Flatmap", types[0], types[1])
        self.fmfunction = fmfunction

    def getWrapper(self):
        udf = self.fmfunction
        def func(iterator):
            return chain.from_iterable(map(udf, iterator))
        return func

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()