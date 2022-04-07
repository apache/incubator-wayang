from pywy.types import T
from typing import (Iterable, Dict, Callable, Any, Generic, Optional)


class GraphNode(Generic[T]):
    current: T
    visited: bool

    def __init__(self, op: T):
        self.current = op
        self.visited = False

    def get_adjacents(self) -> Iterable[T]:
        pass

    def build_node(self, t: T) -> 'GraphNode[T]':
        pass

    def walk(self, created: Dict[T, 'GraphNode[T]']) -> Iterable['GraphNode[T]']:
        adjacent = self.get_adjacents()

        def wrap(op: T):
            if op is None:
                return None
            if op not in created:
                created[op] = self.build_node(op)
            return created[op]

        return map(wrap, adjacent)

    def visit(self,
              parent: 'GraphNode[T]',
              udf: Callable[['GraphNode[T]', 'GraphNode[T]'], Any],
              visit_status: bool = True):
        if self.visited == visit_status:
            return
        self.visited = ~ visit_status
        return udf(self, parent)


class WayangGraph(Generic[T]):
    starting_nodes: Iterable[GraphNode[T]]
    created_nodes: Dict[T, GraphNode[T]]

    def __init__(self, nodes: Iterable[T]):
        self.created_nodes = {}
        start = list()
        for node in nodes:
            tmp = self.build_node(node)
            start.append(tmp)
            self.created_nodes[node] = tmp
        self.starting_nodes = start

    def build_node(self, t: T) -> GraphNode[T]:
        pass

    def traversal(
            self,
            origin: GraphNode[T],
            nodes: Iterable[GraphNode[T]],
            udf: Callable[[GraphNode[T], GraphNode[T]], Any],
            visit_status: bool = True
    ):
        for node in nodes:
            adjacent = node.walk(self.created_nodes)
            self.traversal(node, adjacent, udf, visit_status)
            node.visit(origin, udf)
