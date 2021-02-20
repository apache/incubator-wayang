import abc


class Element(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def accept(self, visitor, udf, orientation, last_iter):
        pass


class Node(Element):
    def __init__(self, operator_type, id, operator):
        self.operator_type = operator_type
        self.id = id
        self.predecessors = {}
        self.successors = {}

        # Temporal
        self.operator = operator

    def add_predecessor(self, id_parent, e):
        self.predecessors[id_parent] = e

    def add_successor(self, id_child, e):
        self.successors[id_child] = e

    def accept(self, visitor, udf, orientation, last_iter):
        visitor.visit_node(self, udf, orientation, last_iter)
