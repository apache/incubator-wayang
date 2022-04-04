from pywayang.types import (GenericTco, Predicate, Function, FlatmapFunction, IterableO)
from pywayang.operator.base import (BaseOperator)
from pywayang.operator.unary import (FilterOperator, MapOperator, FlatmapOperator)


class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    previous : BaseOperator = None

    def __init__(self, operator: BaseOperator):
        self.operator = operator


    def filter(self: "DataQuanta[T]", p: Predicate) -> "DataQuanta[T]" :
        return DataQuanta(FilterOperator(p))

    def map(self: "DataQuanta[I]", f: Function) -> "DataQuanta[O]" :
        return DataQuanta(MapOperator(f))

    def flatmap(self: "DataQuanta[I]", f: FlatmapFunction) -> "DataQuanta[IterableO]" :
        return DataQuanta(FlatmapOperator(f))

    def getOperator(self):
        return self.operator

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
