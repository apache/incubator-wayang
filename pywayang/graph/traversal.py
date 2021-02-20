from graph.visitant import Visitant


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
            print("BAD DEFINED ORIGIN")
            return

        for operator in iter(origin):
            print("operator: ", operator.id)
            node = graph.get_node(operator.id)
            self.app.visit_node(
                node=node,
                udf=self.udf,
                orientation=self.orientation,
                last_iter=None
            )

    def get_collected_data(self):
        return self.app.get_collection()
