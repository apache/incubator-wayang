#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pywy.graph.graph import Graph
from pywy.graph.traversal import Traversal
from pywy.protobuf.planwriter import MessageWriter
from pywy.orchestrator.operator import Operator
import pywy.orchestrator.operator
import itertools
import collections
import logging
from functools import reduce


# Wraps a Source operation to create an iterable
class DataQuantaBuilder:
    def __init__(self, descriptor):
        self.descriptor = descriptor

    def source(self, source):

        if type(source) is str:
            source_ori = open(source, "r")
        else:
            source_ori = source
        return DataQuanta(
            Operator(
                operator_type="source",
                udf=source,
                iterator=iter(source_ori),
                previous=[],
                python_exec=False
            ),
            descriptor=self.descriptor
        )


# Wraps an operation over an iterable
class DataQuanta:
    def __init__(self, operator=None, descriptor=None):
        self.operator = operator
        self.descriptor = descriptor
        if self.operator.is_source():
            self.descriptor.add_source(self.operator)
        if self.operator.is_sink():
            self.descriptor.add_sink(self.operator)

    # Operational Functions
    def filter(self, udf):
        def func(iterator):
            return filter(udf, iterator)

        return DataQuanta(
            Operator(
                operator_type="filter",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    def flatmap(self, udf):

        def auxfunc(iterator):
            return itertools.chain.from_iterable(map(udf, iterator))

        def func(iterator):
            mapped = map(udf, iterator)
            flattened = flatten_single_dim(mapped)
            yield from flattened

        def flatten_single_dim(mapped):
            for item in mapped:
                for subitem in item:
                    yield subitem

        return DataQuanta(
            Operator(
                operator_type="flatmap",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    def group_by(self, udf):
        def func(iterator):
            # TODO key should be given by "udf"
            return itertools.groupby(iterator, key=operator.itemgetter(0))
            #return itertools.groupby(sorted(iterator), key=itertools.itemgetter(0))

        return DataQuanta(
            Operator(
                operator_type="group_by",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    def map(self, udf):
        def func(iterator):
            return map(udf, iterator)

        return DataQuanta(
            Operator(
                operator_type="map",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    # Key specifies pivot dimensions
    # UDF specifies reducer function
    def reduce_by_key(self, keys, udf):

        op = Operator(
            operator_type="reduce_by_key",
            udf=udf,
            previous=[self.operator],
            python_exec=False
        )

        print(len(keys), keys)
        for i in range(0, len(keys)):
            """if keys[i] is int:
                op.set_parameter("vector_position|"+str(i), keys[i])
            else:
                op.set_parameter("dimension_key|"+str(i), keys[i])"""

            # TODO maybe would be better just leave the number as key
            op.set_parameter("dimension|"+str(i+1), keys[i])

        return DataQuanta(
            op,
            descriptor=self.descriptor
        )

    def reduce(self, udf):
        def func(iterator):
            return reduce(udf, iterator)

        return DataQuanta(
            Operator(
                operator_type="reduce",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    def sink(self, path, end="\n"):
        def consume(iterator):
            with open(path, 'w') as f:
                for x in iterator:
                    f.write(str(x) + end)

        def func(iterator):
            consume(iterator)
            # return self.__run(consume)

        return DataQuanta(
            Operator(
                operator_type="sink",

                udf=path,
                # To execute directly uncomment
                # udf=func,

                previous=[self.operator],
                python_exec=False
            ),
            descriptor=self.descriptor
        )

    def sort(self, udf):

        def func(iterator):
            return sorted(iterator, key=udf)

        return DataQuanta(
            Operator(
                operator_type="sort",
                udf=func,
                previous=[self.operator],
                python_exec=True
            ),
            descriptor=self.descriptor
        )

    # This function allow the union to be performed by Python
    # Nevertheless, current configuration runs it over Java
    def union(self, other):

        def func(iterator):
            return itertools.chain(iterator, other.operator.getIterator())

        return DataQuanta(
            Operator(
                operator_type="union",
                udf=func,
                previous=[self.operator, other.operator],
                python_exec=False
            ),
            descriptor=self.descriptor
        )

    def __run(self, consumer):
        consumer(self.operator.getIterator())

    # Execution Functions
    def console(self, end="\n"):
        def consume(iterator):
            for x in iterator:
                print(x, end=end)

        self.__run(consume)

    # Only for debugging purposes!
    # To execute the plan directly in the program driver
    def execute(self):
        logging.warn("DEBUG Execution")
        logging.info("Reminder to swap SINK UDF value from path to func")
        logging.debug(self.operator.previous[0].operator_type)
        if self.operator.is_sink():
            logging.debug(self.operator.operator_type)
            logging.debug(self.operator.udf)
            logging.debug(len(self.operator.previous))
            self.operator.udf(self.operator.previous[0].getIterator())
        else:
            logging.error("Plan must call execute from SINK type of operator")
            raise RuntimeError

    # Converts Python Functional Plan to valid Wayang Plan
    def to_wayang_plan(self):

        sinks = self.descriptor.get_sinks()
        if len(sinks) == 0:
            return

        graph = Graph()
        graph.populate(self.descriptor.get_sinks())

        # Uncomment to check the Graph built
        # graph.print_adjlist()

        # Function to be consumed by Traverse
        # Separates Python Plan into a List of Pipelines
        def define_pipelines(node1, current_pipeline, collection):
            def store_unique(pipe_to_insert):
                for pipe in collection:
                    if equivalent_lists(pipe, pipe_to_insert):
                        return
                collection.append(pipe_to_insert)

            def equivalent_lists(l1, l2):
                if collections.Counter(l1) == collections.Counter(l2):
                    return True
                else:
                    return False

            if not current_pipeline:
                current_pipeline = [node1]

            elif node1.operator.is_boundary():
                store_unique(current_pipeline.copy())
                current_pipeline.clear()
                current_pipeline.append(node1)

            else:
                current_pipeline.append(node1)

            if node1.operator.sink:
                store_unique(current_pipeline.copy())
                current_pipeline.clear()

            return current_pipeline

        # Works over the graph
        trans = Traversal(
            graph=graph,
            origin=self.descriptor.get_sources(),
            # udf=lambda x, y, z: d(x, y, z)
            # UDF always will receive:
            # x: a Node object,
            # y: an object representing the result of the last iteration,
            # z: a collection to store final results inside your UDF
            udf=lambda x, y, z: define_pipelines(x, y, z)
        )

        # Gets the results of the traverse process
        collected_stages = trans.get_collected_data()

        # Passing the Stages to a Wayang message writer
        writer = MessageWriter()
        a = 0
        # Stage is composed of class Node objects
        for stage in collected_stages:
            a += 1
            logging.info("///")
            logging.info("stage" + str(a))
            writer.process_pipeline(stage)

        writer.set_dependencies()

        # Uses a file to provide the plan
        # writer.write_message(self.descriptor)

        # Send the plan to Wayang REST api directly
        writer.send_message(self.descriptor)
