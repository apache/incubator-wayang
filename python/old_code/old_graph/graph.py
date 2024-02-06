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

from old_code.old_graph.node import Node
import logging


# Adjacency Matrix used to analise the plan
class Graph:
    def __init__(self):
        self.graph = {}
        self.nodes_no = 0
        self.nodes = []

    # Fills the Graph
    def populate(self, sinks):
        for sink in iter(sinks):
            self.process_operator(sink)

    # Add current operator and set dependencies
    def process_operator(self, operator):
        self.add_node(operator.operator_type, operator.id, operator)

        if len(operator.previous) > 0:
            for parent in operator.previous:
                if parent:
                    self.add_node(parent.operator_type, parent.id, parent)
                    self.add_link(operator.id, parent.id, 1)
                    self.process_operator(parent)

    def add_node(self, name, id, operator):
        if id in self.nodes:
            return

        self.nodes_no += 1
        self.nodes.append(id)
        new_node = Node(name, id, operator)

        self.graph[id] = new_node

    def add_link(self, id_child, id_parent, e):
        if id_child in self.nodes:
            if id_parent in self.nodes:
                self.graph[id_child].add_predecessor(id_parent, e)
                self.graph[id_parent].add_successor(id_child, e)

    def print_adjlist(self):

        for key in self.graph:
            logging.debug("Node: ", self.graph[key].operator_type, " - ", key)
            for key2 in self.graph[key].predecessors:
                logging.debug("- Parent: ", self.graph[key2].operator_type, " - ", self.graph[key].predecessors[key2], " - ", key2)
            for key2 in self.graph[key].successors:
                logging.debug("- Child: ", self.graph[key2].operator_type, " - ", self.graph[key].successors[key2], " - ", key2)

    def get_node(self, id):
        return self.graph[id]
