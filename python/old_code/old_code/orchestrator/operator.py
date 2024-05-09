#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pickle
import cloudpickle
from old_code.config.config_reader import get_source_types
from old_code.config.config_reader import get_sink_types
from old_code.config.config_reader import get_boundary_types
import logging

pickle_protocol = pickle.HIGHEST_PROTOCOL


# Describes an Operation over an intermediate result
# Each operation could be processed by Python or Java platforms
class Operator:

    def __init__(
        self,
        operator_type=None,
        udf=None,
        previous=None,
        iterator=None,
        python_exec=False
    ):

        # Operator ID
        self.id = id(self)

        # Operator Type
        self.operator_type = operator_type

        # Set Boundaries
        if self.operator_type in get_boundary_types():
            self.boundary = True
        else:
            self.boundary = False

        # UDF Function
        self.udf = udf

        # Source types must come with an Iterator
        self.iterator = iterator
        if operator_type in get_source_types():
            if iterator is None:
                print("Source Operator Type without an Iterator")
                raise
            else:
                self.source = True
        else:
            self.source = False

        # Sink Operators
        if operator_type in get_sink_types():
            self.sink = True
        else:
            self.sink = False

        # TODO Why managing previous and predecessors per separate?
        self.previous = previous

        self.successor = []
        self.predecessor = []

        self.parameters = {}

        # Set predecessors and successors from previous
        if self.previous:
            for prev in self.previous:
                if prev is not None:
                    prev.set_successor(self)
                    self.set_predecessor(prev)

        self.python_exec = python_exec

        logging.info("Operator:" + str(self.getID()) + ", type:" + self.operator_type + ", PythonExecutable: " +
                     str(self.python_exec) +
                     ", is boundary: " + str(self.is_boundary()) + ", is source: " +
                     str(self.source) + ", is sink: " + str(self.sink))

    def getID(self):
        return self.id

    def is_source(self):
        return self.source

    def is_sink(self):
        return self.sink

    def is_boundary(self):
        return self.boundary

    def serialize_udf(self):
        self.udf = cloudpickle.dumps(self.udf)

    def getIterator(self):
        if self.is_source():
            return self.iterator
        # TODO this should iterate through previous REDESIGN
        return self.udf(self.previous[0].getIterator())

    def set_parameter(self, key, value):
        self.parameters[key] = value

    def set_successor(self, suc):
        if (not self.is_sink()) and self.successor.count(suc) == 0:
            self.successor.append(suc)

    def set_predecessor(self, suc):
        if self.predecessor.count(suc) == 0:
            self.predecessor.append(suc)
