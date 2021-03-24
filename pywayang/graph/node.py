#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import abc


class Element(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def accept(self, visitor, udf, orientation, last_iter):
        pass


# Describes an Operator in the Graph
class Node(Element):
    def __init__(self, operator_type, id, operator):
        self.operator_type = operator_type
        self.id = id
        self.predecessors = {}
        self.successors = {}
        self.python_exec = operator.python_exec

        # Temporal
        self.operator = operator

    def add_predecessor(self, id_parent, e):
        self.predecessors[id_parent] = e

    def add_successor(self, id_child, e):
        self.successors[id_child] = e

    # Nodes are visited by objects of class Visitant.
    # Visitants are being used to execute a UDF through the Graph
    def accept(self, visitor, udf, orientation, last_iter):
        visitor.visit_node(self, udf, orientation, last_iter)
