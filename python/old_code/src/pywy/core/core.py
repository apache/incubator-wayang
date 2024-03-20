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

from pywy.core.executor import Executor
from pywy.core.platform import Platform
from pywy.core.mapping import Mapping
from pywy.graph.graph import WayangGraph
from pywy.graph.types import WGraphOfVec, NodeOperator, NodeVec
from pywy.operators import SinkOperator


class TranslateContext:
    """TranslateContext contextual variables a parameters for the translation
    """
    pass


class Plugin:
    """ TODO: enrich this documentation
    A plugin contributes the following components to a :class:`Context`
    - mappings
    - channels
    - configurations
    In turn, it may require several :clas:`Platform`s for its operation.
    """

    platforms: Set[Platform]
    mappings: Mapping
    translate_context: TranslateContext

    def __init__(
            self,
            platforms: Set[Platform],
            mappings: Mapping = Mapping(),
            translate_context: TranslateContext = None):
        self.platforms = platforms
        self.mappings = mappings
        self.translate_context = translate_context

    def get_mappings(self) -> Mapping:
        return self.mappings

    def get_executor(self) -> Executor:
        pass

    def __str__(self):
        return "Platforms: {}, Mappings: {}".format(str(self.platforms), str(self.mappings))

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
        """ Execute the plan with the plugin provided at the moment of creation
        """
        plug = next(iter(self.plugins))
        trs: Translator = Translator(plug, self)
        new_plan = trs.translate()
        plug.get_executor().execute(new_plan)


class Translator:
    """Translator use the :py:class:`pywy.core.Mapping` to convert the :py:class:`pywy.operators.base.PywyOperator`

    Translator take a plan a produce the executable version of the plan using as tool
    the :py:class:`pywy.core.Mapping` of the :py:class:`pywy.core.core.Plugin` and convert
    the :py:class:`pywy.operators.base.PywyOperator` into an executable version inside
    the :py:class:`pywy.core.Platform`

    Attributes
    ----------
    plugin : :py:class:`pywy.core.core.Plugin`
        plugin use in the translation
    plan : :py:class:`pywy.core.core.PywyPlan`
        Plan to be translated by the translator
    translate_context: :py:class:`pywy.core.core.TranslateContext`
        context used by the translates at runtime in some case is not needed
    """

    plugin: Plugin
    plan: PywyPlan
    translate_context: TranslateContext

    def __init__(self, plugin: Plugin, plan: PywyPlan):
        self.plugin = plugin
        self.plan = plan
        self.translate_context = plugin.translate_context

    def translate(self):
        mappings: Mapping = self.plugin.get_mappings()
        graph = WGraphOfVec(self.plan.sinks)

        translate = self.translate_context

        def translate2plugin(current_op: NodeVec, next_op: NodeVec):
            if current_op is None:
                return

            if current_op.current[1] is None:
                current_op.current[1] = mappings.get_instanceof(current_op.current[0], **{'translate_context': translate})

            if next_op is None:
                return
            if next_op.current[1] is None:
                next_op.current[1] = mappings.get_instanceof(next_op.current[0], **{'translate_context': translate})

            # TODO not necesary it it 0
            current_op.current[1].connect(0, next_op.current[1], 0)

        graph.traversal(graph.starting_nodes, translate2plugin)

        node = []
        for elem in graph.starting_nodes:
            node.append(elem.current[1])

        return PywyPlan({self.plugin}, node)
