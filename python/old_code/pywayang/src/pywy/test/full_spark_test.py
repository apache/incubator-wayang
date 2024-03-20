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
import unittest
from pywy.orchestrator.plan import Descriptor
from pywy.orchestrator.dataquanta import DataQuantaBuilder


def test_most_basic(self):
    descriptor = Descriptor()
    descriptor.add_plugin(Descriptor.Plugin.spark)

    plan = DataQuantaBuilder(descriptor)
    sink_dataquanta = \
        plan.source("../test/lines.txt") \
            .sink("../test/output.txt", end="")

    sink_dataquanta.to_wayang_plan()


def test_single_juncture(self):
    descriptor = Descriptor()
    descriptor.add_plugin(Descriptor.Plugin.spark)

    plan = DataQuantaBuilder(descriptor)
    dq_source_a = plan.source("../test/lines.txt")
    dq_source_b = plan.source("../test/morelines.txt")
    sink_dataquanta = dq_source_a.union(dq_source_b) \
        .sink("../test/output.txt", end="")

    sink_dataquanta.to_wayang_plan()


def test_multiple_juncture(self):
    descriptor = Descriptor()
    descriptor.add_plugin(Descriptor.Plugin.spark)

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
