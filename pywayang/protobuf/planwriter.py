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

import protobuf.pywayangplan_pb2 as pwb
import os
import pickle
import cloudpickle
import struct
import base64


class MessageWriter:
    sources = []
    operators = []
    sinks = []
    operator_references = {}
    boundaries = {}

    def add_source(self, operator_id, operator_type, path, predecessors, successors):
        source = pwb.OperatorProto()
        source.id = str(operator_id)
        source.type = operator_type
        source.path = os.path.abspath(path)
        source.udf = chr(0).encode('utf-8')
        #source.predecessors = predecessors
        #source.successors = successors
        self.sources.append(source)
        return source

    def add_sink(self, operator_id, operator_type, path, predecessors, successors):
        sink = pwb.OperatorProto()
        sink.id = str(operator_id)
        sink.type = operator_type
        sink.path = os.path.abspath(path)
        sink.udf = chr(0).encode('utf-8')
        #sink.predecessors = predecessors
        #sink.successors = successors
        self.sinks.append(sink)
        return sink

    def add_operator(self, operator_id, operator_type, udf, path, predecessors, successors):
        op = pwb.OperatorProto()
        op.id = str(operator_id)
        op.type = operator_type
        op.udf = cloudpickle.dumps(udf)
        op.path = str(path)
        #op.predecessors = predecessors
        #op.successors = successors
        self.operators.append(op)
        return op

    # TODO define how dependency will be described
    # should be list of ids
    def process_pipeline(self, stage):

        nested_udf = None
        nested_id = ""
        nested_predecessors = None
        nested_successors = None
        for node in reversed(stage):
            print("########")
            print(node.operator_type, "executable:", node.python_exec, "id:", node.id)

            if not node.python_exec:
                if nested_udf is not None:

                    # Predecessors depends on last operator
                    # Successors depends on first operator
                    op = self.add_operator(
                        nested_id, "map_partition", nested_udf, None,
                        None, None)

                    ids = nested_id.split(",")
                    for id in ids:
                        self.operator_references[id] = op

                    self.boundaries[nested_id] = {}
                    self.boundaries[nested_id]["end"] = nested_successors
                    self.boundaries[nested_id]["start"] = nested_predecessors

                    nested_udf = None
                    nested_id = ""
                    nested_predecessors = None
                    nested_successors = None

                if node.operator.source:
                    op = self.add_source(
                        node.id, node.operator_type, node.operator.udf,
                        node.predecessors, node.operator.successor)
                    self.operator_references[node.id] = op
                    self.boundaries[node.id] = {}
                    self.boundaries[node.id]["end"] = node.successors

                elif node.operator.sink:
                    op = self.add_sink(
                        node.id, node.operator_type, node.operator.udf,
                        node.predecessors, node.operator.successor)
                    self.operator_references[node.id] = op
                    self.boundaries[node.id] = {}
                    self.boundaries[node.id]["start"] = node.predecessors

                # Regular operator to be processed in Java
                else:
                    op = self.add_operator(
                        node.id, node.operator_type, node.operator.udf, None,
                        node.predecessors, node.operator.successor)
                    self.operator_references[node.id] = op
                    self.boundaries[node.id] = {}
                    self.boundaries[node.id]["start"] = node.predecessors
                    self.boundaries[node.id]["end"] = node.successors

            else:

                if nested_udf is None:
                    nested_udf = node.operator.udf
                    nested_id = node.id
                    # It is the last operator to execute in the map partition
                    nested_successors = node.successors

                else:
                    nested_udf = self.concatenate(nested_udf, node.operator.udf)
                    nested_id = str(node.id) + "," + str(nested_id)

                # Every iteration assign the first known predecessors
                nested_predecessors = node.predecessors

        # Just in case in the future some pipelines start with Python operators
        if nested_udf is not None:
            self.add_operator(
                nested_id, "map_partition", nested_udf, None,
                None, None)

            ids = nested_id.split(",")
            for id in ids:
                self.operator_references[id] = op

            self.boundaries[nested_id] = {}
            self.boundaries[nested_id]["end"] = nested_successors
            self.boundaries[nested_id]["start"] = nested_predecessors

    def __init__(self):
        print("lala")

    def concatenate(self, function_a, function_b):
        def executable(iterable):
            return function_a(function_b(iterable))
        return executable

    def old(self, descriptor):

        sink = descriptor.get_sinks()[0]
        source = descriptor.get_sources()[0]

        op = source
        visited = []
        middle_operators = []
        while op.sink is not True and len(op.successor) > 0:
            pre = op.successor[0]
            if pre not in visited and pre.sink is not True:
                pre.serialize_udf()
                middle_operators.append(pre)
            op = pre

        finalpath = "/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/filter_message"
        planconf = pwb.WayangPlan()
        try:
            f = open(finalpath, "rb")
            planconf.ParseFromString(f.read())
            f.close()
        except IOError:
            print(finalpath + ": Could not open file.  Creating a new one.")

        so = pwb.Source()
        so.id = source.id
        so.type = source.operator_type
        so.path = os.path.abspath(source.udf)

        operators = []
        for mid in middle_operators:
            op = pwb.Operator()
            op.id = mid.id
            op.type = mid.operator_type
            op.udf = mid.udf
            operators.append(op)

        si = pwb.Sink()
        si.id = sink.id
        si.type = sink.operator_type
        si.path = os.path.abspath(sink.udf)

        plan = pwb.Plan()
        plan.source.CopyFrom(so)
        plan.sink.CopyFrom(si)
        plan.operators.extend(operators)
        plan.input = pwb.Plan.string
        plan.output = pwb.Plan.string

        ctx = pwb.Context()
        ctx.platforms.extend([pwb.Context.Platform.java])

        planconf.plan.CopyFrom(plan)
        planconf.context.CopyFrom(ctx)

        f = open(finalpath, "wb")
        f.write(planconf.SerializeToString())
        f.close()
        pass

    def pipeline_singleton(self):
        print("lala")
