from typing import Set

from pywy.translate.translator import Translator
from pywy.types import ( GenericTco, Predicate, Function, FlatmapFunction, IterableO )
from pywy.wayangplan import *
from pywy.wayangplan.wayang import PywyPlan
from pywy.platforms.basic.plugin import Plugin

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
  def register(self, p: Plugin):
    self.plugins.add(p)
    return self

  """
  remove a :class:`Plugin` from the :class:`Context`
  """
  def unregister(self, p: Plugin):
    self.plugins.remove(p)
    return self

  def textFile(self, file_path: str) -> 'DataQuanta[str]':
    return DataQuanta(self, TextFileSource(file_path))

  def __str__(self):
    return "Plugins: {}".format(str(self.plugins))

  def __repr__(self):
    return self.__str__()

class DataQuanta(GenericTco):
    """
    Represents an intermediate result/data flow edge in a [[WayangPlan]].
    """
    previous : PywyOperator = None
    context: WayangContext

    def __init__(self, context:WayangContext, operator: PywyOperator):
        self.operator = operator
        self.context = context

    def filter(self: "DataQuanta[T]", p: Predicate) -> "DataQuanta[T]" :
        return DataQuanta(self.context, self.__connect(FilterOperator(p)))

    def map(self: "DataQuanta[I]", f: Function) -> "DataQuanta[O]" :
        return DataQuanta(self.context,self.__connect(MapOperator(f)))

    def flatmap(self: "DataQuanta[I]", f: FlatmapFunction) -> "DataQuanta[IterableO]" :
        return DataQuanta(self.context,self.__connect(FlatmapOperator(f)))

    def storeTextFile(self: "DataQuanta[I]", path: str) :
        last = self.__connect(TextFileSink(path))
        plan = PywyPlan(self.context.plugins, [last])

        trs: Translator =  Translator(self.context.plugins.pop(), plan)
        trs.translate()
        # TODO add the logic to execute the plan

    def __connect(self, op:PywyOperator, port_op: int = 0) -> PywyOperator:
        self.operator.connect(0, op, port_op)
        return op

    def getOperator(self):
        return self.operator

    def __str__(self):
        return str(self.operator)

    def __repr__(self):
        return self.__str__()
