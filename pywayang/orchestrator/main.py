from orchestrator.plan import Descriptor
from orchestrator.dataquanta import DataQuantaBuilder

if __name__ == '__main__':
    # Plan will contain general info about the Rheem Plan created here
    descriptor = Descriptor()

    # We need to save somewhere the sinks and the sources of the plan
    plan = DataQuantaBuilder(descriptor)

    plan.source("../test/lines.txt")\
        .sort(lambda elem: elem.lower())\
        .sink("../test/output.txt", end="")\
        .execute()
