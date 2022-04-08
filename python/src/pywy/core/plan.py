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

from typing import (Iterable, Set)

from pywy.graph.graph import WayangGraph
from pywy.graph.types import (NodeOperator, WGraphOfVec, NodeVec)
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
                "===========\n{}\n@@@@@ => previous is\n{}\n===========\n".format(
                    current.current,
                    previous.current
                )
            )

        self.graph.traversal(self.graph.starting_nodes, print_plan)

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

        self.graph.traversal(self.graph.starting_nodes, print_plan)
