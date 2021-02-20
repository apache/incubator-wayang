from orchestrator.plan import Descriptor
from orchestrator.dataquanta import DataQuantaBuilder


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_sort(descriptor):
    plan = DataQuantaBuilder(descriptor)

    sink_dataquanta = \
        plan.source("../test/lines.txt") \
            .sort(lambda elem: elem.lower()) \
            .sink("../test/output.txt", end="")

    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_basic(descriptor):
    plan = DataQuantaBuilder(descriptor)

    sink_dataquanta = \
        plan.source("../test/lines.txt") \
            .sink("../test/output.txt", end="")

    return sink_dataquanta


if __name__ == '__main__':
    # Plan will contain general info about the Rheem Plan created here
    descriptor = Descriptor()

    plan = plan_basic(descriptor)
    plan.execute()

    print("sources: ")
    for i in descriptor.get_sources():
        print(i.getID(), i.operator_type)

    print("sinks: ")
    for i in descriptor.get_sinks():
        print(i.getID(), i.operator_type)