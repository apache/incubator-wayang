from typing import Set, List, cast

from pywy.core import Translator
from pywy.types import (GenericTco, Predicate, Function, FlatmapFunction, IterableO, T, I, O)
from pywy.operators import *
from pywy.core import PywyPlan
from pywy.core import Plugin


class WayangContext:
    """
    This is the entry point for users to work with Wayang.
    """
    plugins: Set[Plugin]

    def __init__(self):
        self.plugins = set()

    """
    add a :class:`Plugin` to the :class:`Context`
    """

    def register(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.add(p)
        return self

    """
    remove a :class:`Plugin` from the :class:`Context`
    """

    def unregister(self, *plugins: Plugin):
        for p in plugins:
            self.plugins.remove(p)
        return self

    def textfile(self, file_path: str) -> 'DataQuanta[str]':
        return DataQuanta(self, TextFileSource(file_path))

    def __str__(self):
        return "Plugins: {}".format(str(self.plugins))

    def __repr__(self):
        return self.__str__()


class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    context: WayangContext

    def __init__(self, context: WayangContext, operator: PywyOperator):
        self.operator = operator
        self.context = context

    def filter(self: "DataQuanta[T]", p: Predicate) -> "DataQuanta[T]":
        return DataQuanta(self.context, self._connect(FilterOperator(p)))

    def map(self: "DataQuanta[I]", f: Function) -> "DataQuanta[O]":
        return DataQuanta(self.context, self._connect(MapOperator(f)))

    def flatmap(self: "DataQuanta[I]", f: FlatmapFunction) -> "DataQuanta[IterableO]":
        return DataQuanta(self.context, self._connect(FlatmapOperator(f)))

    def store_textfile(self: "DataQuanta[I]", path: str):
        last: List[SinkOperator] = [cast(SinkOperator, self._connect(TextFileSink(path)))]
        plan = PywyPlan(self.context.plugins, last)

        plug = self.context.plugins.pop()
        trs: Translator = Translator(plug, plan)
        new_plan = trs.translate()
        plug.get_executor().execute(new_plan)
        # TODO add the logic to execute the plan

    def _connect(self, op: PywyOperator, port_op: int = 0) -> PywyOperator:
        self.operator.connect(0, op, port_op)
        return op

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
