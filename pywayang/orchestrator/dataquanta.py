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

from orchestrator.operator import Operator
from graph.graph import Graph
from graph.traversal import Traversal
import itertools
import collections


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
                previous=[]
            ),
            descriptor=self.descriptor
        )


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
                previous=[self.operator]
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
                previous=[self.operator]
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

                # udf=path,
                # To execute directly uncomment
                udf=func,

                previous=[self.operator]
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
                previous=[self.operator]
            ),
            descriptor=self.descriptor
        )

    def union(self, other):

        def func(iterator):
            return itertools.chain(iterator, other.operator.getIterator())

        return DataQuanta(
            Operator(
                operator_type="union",
                udf=func,
                previous=[self.operator, other.operator]
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

    def execute(self):
        # print(self.operator.previous[0].operator_type)
        if self.operator.is_sink():
            print(self.operator.operator_type)
            print(self.operator.udf)
            print(len(self.operator.previous))
            self.operator.udf(self.operator.previous[0].getIterator())
        else:
            print("Plan must call execute from SINK type of operator")
            raise RuntimeError

    def unify_pipelines(self):

        sinks = self.descriptor.get_sinks()
        if len(sinks) == 0:
            return

        graph = Graph()
        graph.populate(self.descriptor.get_sinks())

        graph.print_adjlist()

        print("END PRINTING!")

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

        collected_stages = trans.get_collected_data()

        # Setting dependencies

        a = 0
        # Stage is composed of Nodes
        for stage in collected_stages:
            a += 1
            print("///")
            print("stage", a)

            for node in stage:

                print(node.operator_type, node.id)
                print(node.predecessors)

                print(node.successors)
