from typing import Iterable, List

from pywy.graph.graph import GraphNode, WayangGraph
from pywy.wayangplan.base import WyOperator

class WayangNode(GraphNode[WyOperator]):

    def __init__(self, op: WyOperator):
        super(WayangNode, self).__init__(op)

    def getadjacents(self) -> Iterable[WyOperator]:
        operator: WyOperator = self.current
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t:WyOperator) -> 'WayangNode':
        return WayangNode(t)

class WayangGraphOfWayangNode(WayangGraph[WayangNode]):

    def __init__(self, nodes: List[WyOperator]):
        super(WayangGraphOfWayangNode, self).__init__(nodes)

    def build_node(self, t:WyOperator) -> WayangNode:
        return WayangNode(t)
