import unittest
from orchestrator.plan import Descriptor
from orchestrator.dataquanta import DataQuantaBuilder


class MyTestCase(unittest.TestCase):

    def test_most_basic(self):
        descriptor = Descriptor()
        plan = DataQuantaBuilder(descriptor)

        sink_dataquanta = \
            plan.source("../test/lines.txt") \
                .sink("../test/output.txt", end="")

        sink_dataquanta.to_wayang_plan()


    def test_single_juncture(self):
        descriptor = Descriptor()
        plan = DataQuantaBuilder(descriptor)

        dq_source_a = plan.source("../test/lines.txt")
        dq_source_b = plan.source("../test/morelines.txt")
        sink_dataquanta = dq_source_a.union(dq_source_b) \
            .sink("../test/output.txt", end="")

        sink_dataquanta.to_wayang_plan()


    def test_multiple_juncture(self):
        descriptor = Descriptor()
        plan = DataQuantaBuilder(descriptor)
        dq_source_a = plan.source("../test/lines.txt")
        dq_source_b = plan.source("../test/morelines.txt") \
            .filter(lambda elem: str(elem).startswith("I"))
        dq_source_c = plan.source("../test/lastlines.txt") \
            .filter(lambda elem: str(elem).startswith("W"))

        sink_dataquanta = dq_source_a.union(dq_source_b) \
            .union(dq_source_c) \
            .sort(lambda elem: elem.lower()) \
            .sink("../test/output.txt", end="")

        sink_dataquanta.to_wayang_plan()


if __name__ == '__main__':
    unittest.main()
