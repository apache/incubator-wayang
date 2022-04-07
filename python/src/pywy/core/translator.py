from pywy.graph.types import (WGraphOfVec, NodeVec)
from pywy.core.plugin import Plugin
from pywy.core.plan import PywyPlan
from pywy.core.mapping import Mapping


class Translator:

    plugin: Plugin
    plan: PywyPlan

    def __init__(self, plugin: Plugin, plan: PywyPlan):
        self.plugin = plugin
        self.plan = plan

    def translate(self):
        mappings: Mapping = self.plugin.get_mappings()
        graph = WGraphOfVec(self.plan.sinks)

        def translate2plugin(current_op: NodeVec, next_op: NodeVec):
            if current_op is None:
                return

            if current_op.current[1] is None:
                current_op.current[1] = mappings.get_instanceof(current_op.current[0])

            if next_op is None:
                return
            if next_op.current[1] is None:
                next_op.current[1] = mappings.get_instanceof(next_op.current[0])

            # TODO not necesary it it 0
            current_op.current[1].connect(0, next_op.current[1], 0)

        graph.traversal(None, graph.starting_nodes, translate2plugin)

        node = []
        for elem in graph.starting_nodes:
            node.append(elem.current[1])

        return PywyPlan({self.plugin}, node)
