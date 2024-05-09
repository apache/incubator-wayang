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

from pywy.orchestrator.plan import Descriptor
from pywy.orchestrator.dataquanta import DataQuantaBuilder
import datetime


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


def plan_tpch_q1(descriptor):

    # TODO create reduce by
    plan = DataQuantaBuilder(descriptor)

    def reducer(obj1, obj2):
        return obj1[0], obj1[1], obj1[2] + obj2[2], obj1[3] + obj2[3], obj1[4] + obj2[4], obj1[5] + obj2[5], \
               obj1[6] + obj2[6], obj1[7] + obj2[7], obj1[8] + obj2[8], obj1[9] + obj2[9]

    sink = plan.source("../test/lineitem.txt") \
        .map(lambda elem: elem.split("|")) \
        .sink("../test/output.txt", end="")
    """
        .filter(lambda elem: datetime.datetime.strptime(elem[10], '%Y-%m-%d') <= datetime.datetime.strptime('1998-09-02', '%Y-%m-%d')) \
        .map(lambda elem:
             [elem[8], elem[9], elem[4], elem[5],
              float(elem[5]) * (1 - float(elem[6])),
              float(elem[5]) * (1 - float(elem[6])) * (1 + float(elem[7])),
              elem[4], elem[5],
              elem[6], 1]) \
        .sink("../test/output.txt", end="")"""
        # .reduce_by_key([0, 1], reducer) \


    return sink


def plan_full_java(descriptor):

    plan = DataQuantaBuilder(descriptor)

    dq_source_a = plan.source("../test/lines.txt")
    dq_source_b = plan.source("../test/morelines.txt")
    sink_dataquanta = dq_source_a.union(dq_source_b) \
        .sink("../test/output.txt", end="")

    return sink_dataquanta


def plan_wordcount(descriptor):

    plan = DataQuantaBuilder(descriptor)
    sink_wordcount = plan.source("../test/lineitem.txt") \
        .filter(lambda elem: len(str(elem).split("|")[0]) < 4) \
        .flatmap(lambda elem: str(elem).split("|")) \
        .sink("../test/output.txt", end="")

    return sink_wordcount


if __name__ == '__main__':

    # Plan will contain general info about the Wayang Plan created here
    descriptor = Descriptor()
    descriptor.add_plugin(Descriptor.Plugin.spark)
    descriptor.add_plugin(Descriptor.Plugin.java)

    plan_dataquanta_sink = plan_wordcount(descriptor)
    # plan_dataquanta_sink.execute()
    # plan_dataquanta_sink.console()

    plan_dataquanta_sink.to_wayang_plan()
