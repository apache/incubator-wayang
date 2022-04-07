from itertools import chain
from pywy.operators.base import PywyOperator
from pywy.types import (
                            GenericTco,
                            GenericUco,
                            Predicate,
                            get_type_predicate,
                            Function,
                            get_type_function,
                            FlatmapFunction,
                            get_type_flatmap_function
                        )


class UnaryToUnaryOperator(PywyOperator):

    def __init__(self, name: str, input_type: GenericTco, output_type: GenericUco):
        super().__init__(name, input_type, output_type, 1, 1)

    def postfix(self) -> str:
        return 'OperatorUnary'

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class FilterOperator(UnaryToUnaryOperator):

    predicate: Predicate

    def __init__(self, predicate: Predicate):
        predicate_type = get_type_predicate(predicate) if predicate else None
        super().__init__("Filter", predicate_type, predicate_type)
        self.predicate = predicate

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()


class MapOperator(UnaryToUnaryOperator):

    function: Function

    def __init__(self, function: Function):
        types = get_type_function(function) if function else (None, None)
        super().__init__("Map", types[0], types[1])
        self.function = function

    # TODO remove wrapper
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

    fm_function: FlatmapFunction

    def __init__(self, fm_function: FlatmapFunction):
        types = get_type_flatmap_function(fm_function) if fm_function else (None, None)
        super().__init__("Flatmap", types[0], types[1])
        self.fm_function = fm_function

    # TODO remove wrapper
    def getWrapper(self):
        udf = self.fm_function
        def func(iterator):
            return chain.from_iterable(map(udf, iterator))
        return func

    def __str__(self):
        return super().__str__()

    def __repr__(self):
        return super().__repr__()
