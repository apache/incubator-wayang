class Descriptor:

    def __init__(self):
        self.sinks = []
        self.sources = []
        self.boundary_operators = None

    def get_boundary_operators(self):
        return self.boundary_operators

    def add_source(self, operator):
        self.sources.append(operator)

    def get_sources(self):
        return self.sources

    def add_sink(self, operator):
        self.sinks.append(operator)

    def get_sinks(self):
        return self.sinks