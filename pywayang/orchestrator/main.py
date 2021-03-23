#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from orchestrator.plan import Descriptor
from orchestrator.dataquanta import DataQuantaBuilder


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_sort(descriptor):
    plan = DataQuantaBuilder(descriptor)
    sink_dataquanta = \
        plan.source("../test/words.txt") \
            .sort(lambda elem: elem.lower()) \
            .sink("../test/output.txt", end="")
    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_sort_filter(descriptor):
    plan = DataQuantaBuilder(descriptor)
    sink_dataquanta = \
        plan.source("../test/words.txt") \
            .sort(lambda elem: elem.lower()) \
            .filter(lambda elem: str(elem).startswith("f")) \
            .sink("../test/output.txt", end="")
    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_filter_text(descriptor):
    plan = DataQuantaBuilder(descriptor)

    sink_dataquanta = \
        plan.source("../test/words.txt") \
            .filter(lambda elem: str(elem).startswith("f")) \
            .sink("../test/output.txt", end="")

    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_filter(descriptor):
    plan = DataQuantaBuilder(descriptor)

    sink_dataquanta = \
        plan.source("../test/numbers.txt") \
            .filter(lambda elem: int(elem) % 2 != 0) \
            .sink("../test/output.txt", end="")

    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_basic(descriptor):
    plan = DataQuantaBuilder(descriptor)

    sink_dataquanta = \
        plan.source("../test/lines.txt") \
            .sink("../test/output.txt", end="")

    return sink_dataquanta


# Returns the Sink Executable Dataquanta of a DEMO plan
def plan_junction(descriptor):

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

    return sink_dataquanta


def plan_java_junction(descriptor):

    plan = DataQuantaBuilder(descriptor)

    dq_source_a = plan.source("../test/lines.txt")
    dq_source_b = plan.source("../test/morelines.txt")
    sink_dataquanta = dq_source_a.union(dq_source_b) \
        .filter(lambda elem: str(elem).startswith("I")) \
        .sort(lambda elem: elem.lower()) \
        .sink("../test/output.txt", end="")

    return sink_dataquanta


def plan_full_java(descriptor):

    plan = DataQuantaBuilder(descriptor)

    dq_source_a = plan.source("../test/lines.txt")
    dq_source_b = plan.source("../test/morelines.txt")
    sink_dataquanta = dq_source_a.union(dq_source_b) \
        .sink("../test/output.txt", end="")

    return sink_dataquanta


if __name__ == '__main__':

    # Plan will contain general info about the Wayang Plan created here
    descriptor = Descriptor()

    plan_dataquanta_sink = plan_java_junction(descriptor)
    # plan_dataquanta_sink.execute()
    # plan_dataquanta_sink.console()

    plan_dataquanta_sink.to_wayang_plan()
