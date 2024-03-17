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

from typing import Set, Iterable
import json

from pywy.core.platform import Platform
from pywy.graph.graph import WayangGraph
from pywy.graph.types import WGraphOfVec, NodeOperator, NodeVec
from pywy.operators import SinkOperator


class Plugin:
    """ TODO: enrich this documentation
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms: Set[Platform]

    def __init__(
            self,
            platforms: Set[Platform]):
        self.platforms = platforms

    def __str__(self):
        return "Platforms: {}".format(str(self.platforms))

    def __repr__(self):
        return self.__str__()


class PywyPlan:
    """A PywyPlan consists of a set of :py:class:`pywy.operators.base.PywyOperator`

    the operator inside PywyPlan follow a Directed acyclic graph(DAG), and describe
    how the execution needs to be performed

    Attributes
    ----------
    graph : :py:class:`pywy.graph.graph.WayangGraph`
       Graph that describe the DAG, and it provides the iterable properties to
       the PywyPlan
    plugins : :obj:`set` of :py:class:`pywy.core.plugin.Plugin`
        plugins is the set of possible platforms that can be uses to execute
        the PywyPlan
    sinks : :py:class:`typing.Iterable` of :py:class:`pywy.operators.sink.SinkOperator`
        The list of sink operators, this describe the end of the pipeline, and
        they are used to build the `graph`
    """
    graph: WayangGraph

    def __init__(self, plugins: Set[Plugin], sinks: Iterable[SinkOperator]):
        """basic Constructor of PywyPlan

        this constructor set the plugins and sinks element, and it prepares
        everything for been executed

        Parameters
        ----------
        plugins
            Description of `plugins`.
        sinks
            Description of `sinks`.
        """
        self.plugins = plugins
        self.sinks = sinks
        self.set_graph()

    def set_graph(self):
        """ it builds the :py:class:`pywy.graph.graph.WayangGraph` of the current PywyPlan
        """
        self.graph = WGraphOfVec(self.sinks)

    def execute(self):
        """ Transform the plan into json objects to send it to Wayang
        """
        json_data = {}
        context = {}
        context["origin"] = "python"
        context["platforms"] = {}

        if len(self.plugins) > 0:
            context["platforms"] = list(map(lambda pl: next(iter(pl.platforms)).name, self.plugins))

        json_data["context"] = context
        json_data["operators"] = []

        nodes = []
        self.graph.traversal(self.graph.starting_nodes, lambda x, parent: nodes.append(x))
        id_table = {(obj.current[0]): index + 1 for index, obj in enumerate(nodes)}

        for node in nodes:
            operator = node.current[0]
            json_operator = {}
            json_operator["id"] = id_table[operator]
            json_operator["operatorName"] = operator.name
            json_operator["cat"] = operator.cat
            if operator.cat != "input":
                json_operator["input"] = list(map(lambda x: id_table[x], operator.inputOperator))
            else:
                json_operator["input"] = []

            if operator.cat != "output":
                json_operator["output"] = list(map(lambda x: id_table[x], operator.outputOperator))
            else:
                json_operator["output"] = []
            json_data["operators"].append(json_operator)

        print(json_data)
        """Now send the json_data to the running REST API process
        """
