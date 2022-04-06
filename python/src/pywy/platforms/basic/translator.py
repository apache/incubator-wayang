from pywy.graph.graphtypes import ( WGraphOfVec, NodeVec )
from pywy.platforms.basic.plugin import Plugin
from pywy.platforms.basic.plan import PywyPlan
from pywy.platforms.basic.mapping import Mapping

class Translator:

    plugin: Plugin
    plan : PywyPlan

    def __init__(self, plugin: Plugin, plan: PywyPlan):
        self.plugin = plugin
        self.plan = plan

    def translate(self):
        mappings:Mapping = self.plugin.get_mappings()
        graph = WGraphOfVec(self.plan.sinks)
        def translate2plugin(current: NodeVec, next: NodeVec):
            if current is None:
                return

            if current.current[1] is None:
                current.current[1] = mappings.get_instanceof(current.current[0])

            if next is None:
                return
            if next.current[1] is None:
                next.current[1] = mappings.get_instanceof(next.current[0])

            # TODO not necesary it it 0
            current.current[1].connect(0, next.current[1], 0)

        graph.traversal(None, graph.starting_nodes, translate2plugin)

        # def print_plan(current: NodeVec, previous: NodeVec):
        #     if current is None:
        #         print("this is source")
        #         print(previous.current)
        #         return
        #     if previous is None:
        #         print("this is sink")
        #         print(current.current)
        #         return
        #
        #     print(
        #         "############\n{}\n@@@@@ => previous is\n{}\n############\n"
        #             .format(
        #                 current.current,
        #                 previous.current
        #              )
        #     )
        #
        # graph.traversal(None, graph.starting_nodes, print_plan, False)

        node = []
        for elem in graph.starting_nodes:
            node.append(elem.current[1])

        return PywyPlan(self.plugin, node)