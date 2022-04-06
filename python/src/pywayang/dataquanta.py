from pywayang.types import (GenericTco, Predicate, Function, FlatmapFunction, IterableO)
from pywayang.operator.base import (WyOperator)
from pywayang.operator.unary import (FilterOperator, MapOperator, FlatmapOperator)
from pywayang.operator.sink import TextFileSink


class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    previous : WyOperator = None

    def __init__(self, operator: WyOperator):
        self.operator = operator


    def filter(self: "DataQuanta[T]", p: Predicate) -> "DataQuanta[T]" :
        return DataQuanta(FilterOperator(p))

    def map(self: "DataQuanta[I]", f: Function) -> "DataQuanta[O]" :
        return DataQuanta(MapOperator(f))

    def flatmap(self: "DataQuanta[I]", f: FlatmapFunction) -> "DataQuanta[IterableO]" :
        return DataQuanta(FlatmapOperator(f))

    def storeTextFile(self: "DataQuanta[I]", path: str) :
        last = DataQuanta(TextFileSink(path))
        # TODO add the logic to execute the plan

    def getOperator(self):
        return self.operator

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
