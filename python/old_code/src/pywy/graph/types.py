#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from typing import (Iterable, List)

from pywy.graph.graph import (GraphNode, WayangGraph)
from pywy.operators.base import PywyOperator, PO_T


class NodeOperator(GraphNode[PO_T, PO_T]):

    def __init__(self, op: PO_T):
        super(NodeOperator, self).__init__(op)

    def get_adjacents(self) -> List[PO_T]:
        operator: PO_T = self.current
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t: PO_T) -> 'NodeOperator':
        return NodeOperator(t)


class WGraphOfOperator(WayangGraph[PO_T, NodeOperator]):

    def __init__(self, nodes: Iterable[PO_T]):
        super(WGraphOfOperator, self).__init__(nodes)

    def build_node(self, t: PO_T) -> NodeOperator:
        return NodeOperator(t)


class NodeVec(GraphNode[PO_T, List[PO_T]]):

    def __init__(self, op: PO_T):
        super(NodeVec, self).__init__([op, None])

    def get_adjacents(self) -> List[PO_T]:
        operator: PO_T = self.current[0]
        if operator is None or operator.inputs == 0:
            return []
        return operator.inputOperator

    def build_node(self, t: PO_T) -> 'NodeVec':
        return NodeVec(t)

    def __str__(self):
        return "NodeVec {}".format(self.current)

    def __repr__(self):
        return self.__str__()


class WGraphOfVec(WayangGraph[PO_T, NodeVec]):

    def __init__(self, nodes: Iterable[PO_T]):
        super(WGraphOfVec, self).__init__(nodes)

    def build_node(self, t: PO_T) -> NodeVec:
        return NodeVec(t)
