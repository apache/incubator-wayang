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

from __future__ import annotations

from pywy.types import T, K
from typing import (Iterable, Dict, Callable, Any, Generic, Optional, List)


class GraphNode(Generic[K, T]):
    current: T
    visited: bool

    def __init__(self, op: T):
        self.current = op
        self.visited = False

    def get_adjacents(self) -> List[K]:
        pass

    def build_node(self, t: T) -> 'GraphNode[K, T]':
        pass

    def walk(self, created: Dict[K, 'GraphNode[K, T]']) -> Iterable['GraphNode[K, T]']:
        adjacent = self.get_adjacents()

        if len(adjacent) == 0:
            return []

        def wrap(op: T) -> Optional['GraphNode[K, T]'] | None:
            if op is None:
                return None
            if op not in created:
                created[op] = self.build_node(op)
            return created[op]

        return map(wrap, adjacent)

    def visit(self,
              parent: 'GraphNode[K, T]',
              udf: Callable[['GraphNode[K, T]', 'GraphNode[K, T]'], Any],
              visit_status: bool = True):
        if self.visited == visit_status:
            return
        self.visited = ~ visit_status
        return udf(self, parent)


class WayangGraph(Generic[K, T]):
    starting_nodes: Iterable[GraphNode[K, T]]
    created_nodes: Dict[K, GraphNode[K, T]]

    def __init__(self, nodes: Iterable[T]):
        self.created_nodes = {}
        start = list()
        for node in nodes:
            tmp = self.build_node(node)
            start.append(tmp)
            self.created_nodes[node] = tmp
        self.starting_nodes = start

    def build_node(self, t: T) -> GraphNode[K, T]:
        pass

    def traversal(
            self,
            nodes: Iterable[GraphNode[K, T]],
            udf: Callable[[GraphNode[K, T], GraphNode[K, T]], Any],
            origin: Optional[GraphNode[K, T]] = None,
            visit_status: bool = True
    ):
        for node in nodes:
            adjacent = node.walk(self.created_nodes)
            self.traversal(adjacent, udf, node, visit_status)
            node.visit(origin, udf)
