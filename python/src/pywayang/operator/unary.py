from pywayang.operator.base import BaseOperator
from pywayang.types import (
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


class UnaryToUnaryOperator(BaseOperator):

    def __init__(self, name:str, input:GenericTco, output:GenericUco):
        super().__init__(name, input, output, 1, 1)

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()



class FilterOperator(UnaryToUnaryOperator):

    predicate: Predicate

    def __init__(self, predicate: Predicate):
        type = getTypePredicate(predicate)
        super().__init__("FilterOperator", type, type)
        self.predicate = predicate

    def getWrapper(self):
        udf = self.predicate
        def func(iterator):
            return filter(udf, iterator)
        return func

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()

class MapOperator(UnaryToUnaryOperator):

    function: Function

    def __init__(self, function: Function):
        types = getTypeFunction(function)
        super().__init__("MapOperator", types[0], types[1])
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
        types = getTypeFlatmapFunction(fmfunction)
        super().__init__("FlatmapOperator", types[0], types[1])
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