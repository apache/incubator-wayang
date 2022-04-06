from typing import Iterable, Dict, Callable, NoReturn, List, Set

from pywy.wayangplan.sink import SinkOperator
from pywy.wayangplan.base import WyOperator
from pywy.platforms.basic.plugin import Plugin

class GraphNodeWayang:

    current: WyOperator
    visited: bool

    def __init__(self, op: WyOperator):
        self.current = op
        self.visited = False

    def successors(self, created: Dict[WyOperator, 'GraphNodeWayang']) -> Iterable['GraphNodeWayang']:
        if self.current is None or self.current.outputs == 0:
            return []

        def wrap(op:WyOperator):
            if op is None:
                return None;
            if op not in created:
                created[op] = GraphNodeWayang(op)
            return created[op]

        adjacent = self.current.outputOperator
        return map(wrap, adjacent)

    def predecessors(self, created: Dict[WyOperator, 'GraphNodeWayang']) -> Iterable['GraphNodeWayang']:
        print("predecessors")
        print(self)
        def wrap(op:WyOperator):
            if op not in created:
                created[op] = GraphNodeWayang(op)
            return created[op]

        adjacent = self.current.inputOperator
        return map(wrap, adjacent)

    def visit(self, parent: 'GraphNodeWayang', udf: Callable[['GraphNodeWayang', 'GraphNodeWayang'], NoReturn], visit_status: bool = True):
        if(self.visited == visit_status):
            return
        udf(self, parent)
        self.visited = visit_status

class GraphWayang:

    starting_nodes : List[GraphNodeWayang]
    created_nodes : Dict[WyOperator, GraphNodeWayang]

    def __init__(self, plan:'PywyPlan'):
        self.created_nodes = {}
        self.starting_nodes = list()
        for sink in plan.sinks:
            tmp = GraphNodeWayang(sink)
            self.starting_nodes.append(tmp)
            self.created_nodes[sink] = tmp


    def traversal(
            self,
            origin: GraphNodeWayang,
            nodes: Iterable[GraphNodeWayang],
            udf: Callable[['GraphNodeWayang', 'GraphNodeWayang'], NoReturn]
    ):
        for node in nodes:
            adjacents = node.predecessors(self.created_nodes)
            self.traversal(node, adjacents, udf)
            node.visit(origin, udf)

class PywyPlan:

    graph: GraphWayang

    def __init__(self, plugins: Set[Plugin], sinks: Iterable[SinkOperator]):
        self.plugins = plugins
        self.sinks = sinks
        self.graph = GraphWayang(self)

    def print(self):
        def print_plan(current: GraphNodeWayang, previous: GraphNodeWayang):
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



