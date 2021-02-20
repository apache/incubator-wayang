import abc


class Visitor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def visit_node(self, node, udf, orientation, last_iter):
        pass


# Applies UDF in current Node
class Visitant(Visitor):

    def __init__(self, graph, results):
        self.collection = results
        self.graph = graph

    # UDF can store results in ApplyFunction.collection whenever its requires.
    # last_iter has the generated current value obtained in the previous iteration
    def visit_node(self, node, udf, orientation, last_iter):
        # print("Applying UDf", orientation)
        current_value = udf(node, last_iter, self.collection)
        # print("orientation result ", getattr(node, orientation))
        next_iter = getattr(node, orientation)
        if len(next_iter) > 0:
            for next_iter_id in next_iter:
                if next_iter_id:
                    # print("parent_id: ", next_iter_id)
                    next_iter_node = self.graph.get_node(next_iter_id)
                    # print("next_iter_node: ", next_iter_node.kind)
                    next_iter_node.accept(visitor=self, udf=udf, orientation=orientation, last_iter=current_value)
        pass

    def get_collection(self):
        return self.collection
