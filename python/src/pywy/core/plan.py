from typing import ( Iterable, Set )

from pywy.graph.graph import WayangGraph
from pywy.graph.types import ( NodeOperator, WGraphOfVec, NodeVec )
from pywy.operators.sink import SinkOperator
from pywy.core.plugin import Plugin


class PywyPlan:

    graph: WayangGraph

    def __init__(self, plugins: Set[Plugin], sinks: Iterable[SinkOperator]):
        self.plugins = plugins
        self.sinks = sinks
        self.set_graph()

    def set_graph(self):
        self.graph = WGraphOfVec(self.sinks)

    def print(self):
        def print_plan(current: NodeOperator, previous: NodeOperator):
            if current is None:
                print("this is source")
                print(previous.current)
                return
            if previous is None:
                print("this is sink")
                print(current.current)
                return

            print(
                "===========\n{}\n@@@@@ => previous is\n{}\n===========\n"
                    .format(
                        current.current,
                        previous.current
                     )
            )
        self.graph.traversal(None, self.graph.starting_nodes, print_plan)


    def printTuple(self):
        def print_plan(current: NodeVec, previous: NodeVec):
            if current is None:
                print("this is source")
                print(previous.current)
                return
            if previous is None:
                print("this is sink")
                print(current.current)
                return

            print(
                "############\n{}\n@@@@@ => previous is\n{}\n############\n"
                    .format(
                        current.current,
                        previous.current
                     )
            )
        self.graph.traversal(None, self.graph.starting_nodes, print_plan)

