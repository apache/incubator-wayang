from typing import ( Iterable, List )

from pywy.graph.graph import ( GraphNode, WayangGraph )
from pywy.wayangplan.base import WyOperator

class NodeOperator(GraphNode[WyOperator]):

    def __init__(self, op: WyOperator):
        super(NodeOperator, self).__init__(op)

    def getadjacents(self) -> Iterable[WyOperator]:
        operator: WyOperator = self.current
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t:WyOperator) -> 'NodeOperator':
        return NodeOperator(t)

class WGraphOfOperator(WayangGraph[NodeOperator]):

    def __init__(self, nodes: List[WyOperator]):
        super(WGraphOfOperator, self).__init__(nodes)

    def build_node(self, t:WyOperator) -> NodeOperator:
        return NodeOperator(t)


class NodeVec(GraphNode[List[WyOperator]]):

    def __init__(self, op: WyOperator):
        super(NodeVec, self).__init__([op, None])

    def getadjacents(self) -> Iterable[List[WyOperator]]:
        operator: WyOperator = self.current[0]
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t:WyOperator) -> 'NodeVec':
        return NodeVec(t)

    def __str__(self):
        return "NodeVec {}".format(self.current)

    def __repr__(self):
        return self.__str__()

class WGraphOfVec(WayangGraph[NodeVec]):

    def __init__(self, nodes: List[WyOperator]):
        super(WGraphOfVec, self).__init__(nodes)

    def build_node(self, t:WyOperator) -> NodeVec:
        return NodeVec(t)