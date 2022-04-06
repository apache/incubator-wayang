from pywy.types import T
from typing import Iterable, Dict, Callable, List, Any, Generic


class GraphNode(Generic[T]):

    current: T
    visited: bool

    def __init__(self, op: T):
        self.current = op
        self.visited = False

    def getadjacents(self) -> Iterable[T]:
        pass

    def build_node(self, t:T) -> 'GraphNode[T]':
        pass

    def adjacents(self, created: Dict[T, 'GraphNode[T]']) -> Iterable['GraphNode[T]']:
        adjacent = self.getadjacents()

        if len(adjacent) == 0:
            return []

        def wrap(op:T):
            if op is None:
                return None
            if op not in created:
                created[op] = self.build_node(op)
            return created[op]

        return map(wrap, adjacent)

    def visit(self, parent: 'GraphNode[T]', udf: Callable[['GraphNode[T]', 'GraphNode[T]'], Any], visit_status: bool = True):
        if(self.visited == visit_status):
            return
        self.visited = visit_status
        return udf(self, parent)


class WayangGraph(Generic[T]):

    starting_nodes : List[GraphNode[T]]
    created_nodes : Dict[T, GraphNode[T]]

    def __init__(self, nodes: List[T]):
        self.created_nodes = {}
        self.starting_nodes = list()
        for node in nodes:
            tmp = self.build_node(node)
            self.starting_nodes.append(tmp)
            self.created_nodes[node] = tmp

    def build_node(self, t:T) -> GraphNode[T]:
        pass

    def traversal(
            self,
            origin: GraphNode[T],
            nodes: Iterable[GraphNode[T]],
            udf: Callable[['GraphNode[T]', 'GraphNode[T]'], Any]
    ):
        for node in nodes:
            adjacents = node.adjacents(self.created_nodes)
            self.traversal(node, adjacents, udf)
            node.visit(origin, udf)