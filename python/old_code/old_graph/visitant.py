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

import abc
import logging


class Visitor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def visit_node(self, node, udf, orientation, last_iter):
        pass


# Applies a UDF in current Node
class Visitant(Visitor):

    def __init__(self, graph, results):
        self.collection = results
        self.graph = graph

    # UDF can store results in ApplyFunction.collection whenever its requires.
    # last_iter has the generated current value obtained in the previous iteration
    def visit_node(self, node, udf, orientation, last_iter):
        logging.debug("Applying UDf" + str(orientation))
        current_value = udf(node, last_iter, self.collection)
        logging.debug("orientation result " + str(getattr(node, orientation)))
        next_iter = getattr(node, orientation)
        if len(next_iter) > 0:
            for next_iter_id in next_iter:
                if next_iter_id:
                    logging.debug("next_id: " + str(next_iter_id))
                    next_iter_node = self.graph.get_node(next_iter_id)
                    logging.debug("next_iter_node: " + next_iter_node.operator_type + " " + str(next_iter_node.id))
                    next_iter_node.accept(visitor=self, udf=udf, orientation=orientation, last_iter=current_value)
        pass

    def get_collection(self):
        return self.collection
