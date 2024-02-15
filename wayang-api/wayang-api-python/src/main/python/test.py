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
import tempfile
import logging
import os
from typing import List, Iterable

from pywy.orchestrator.plan import Descriptor
from pywy.orchestrator.dataquanta import DataQuantaBuilder


def test_single_juncture():
    descriptor = Descriptor()
    descriptor.add_plugin(Descriptor.Plugin.java)

    plan = DataQuantaBuilder(descriptor)
    dq_source_a = plan.source("../resources/test.input")
    dq_source_b = plan.source("../resources/text.input")
    sink_dataquanta = dq_source_a.union(dq_source_b) \
        .sink("../resources/output.txt", end="")

    sink_dataquanta.to_wayang_plan()

if __name__ == '__main__':
    test_single_juncture()
