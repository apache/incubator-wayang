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

from old_code.old_graph.visitant import Visitant
import logging


# Defines how a UDF will be applied over the Graph
class Traversal:

    def __init__(self, graph, origin, udf):
        self.graph = graph
        self.origin = origin
        self.udf = udf
        self.app = Visitant(graph, [])

        # Starting from Sinks or Sources sets an specific orientation
        if origin[0].source:
            self.orientation = "successors"
        elif origin[0].sink:
            self.orientation = "predecessors"
        else:
            logging.error("Origin point to traverse the plan wrongly defined")
            return

        for operator in iter(origin):
            logging.debug("operator origin: " + str(operator.id))
            node = graph.get_node(operator.id)
            self.app.visit_node(
                node=node,
                udf=self.udf,
                orientation=self.orientation,
                last_iter=None
            )

    def get_collected_data(self):
        return self.app.get_collection()
