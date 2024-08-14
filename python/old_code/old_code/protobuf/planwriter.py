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

import old_code.protobuf.pywayangplan_pb2 as pwb
import os
import cloudpickle
import logging
import requests
import base64


# Writes Wayang Plan from several stages
class MessageWriter:
    sources = []
    operators = []
    sinks = []
    operator_references = {}
    boundaries = {}

    # Creates and appends Source type of operator
    def add_source(self, operator_id, operator_type, path):
        source = pwb.OperatorProto()
        source.id = str(operator_id)
        source.type = operator_type
        source.path = os.path.abspath(path)
        source.udf = chr(0).encode('utf-8')
        # source.parameters = {}
        self.sources.append(source)
        return source

    # Creates and appends Sink type of operator
    def add_sink(self, operator_id, operator_type, path):
        sink = pwb.OperatorProto()
        sink.id = str(operator_id)
        sink.type = operator_type
        sink.path = os.path.abspath(path)
        sink.udf = chr(0).encode('utf-8')
        # sink.parameters = {}
        self.sinks.append(sink)
        return sink

    # Creates and appends a Python operator
    # Python OP don't require parameters, UDF has the function ready to be executed directly
    def add_operator(self, operator_id, operator_type, udf):
        op = pwb.OperatorProto()
        op.id = str(operator_id)
        op.type = operator_type
        op.udf = cloudpickle.dumps(udf)
        op.path = str(None)
        # op.parameters = {}
        self.operators.append(op)
        return op

    # Creates and appends a Java operator
    def add_java_operator(self, operator_id, operator_type, udf, parameters):
        op = pwb.OperatorProto()
        op.id = str(operator_id)
        op.type = operator_type
        op.udf = cloudpickle.dumps(udf)
        op.path = str(None)
        #op.parameters = parameters
        for param in parameters:
            print(param, parameters[param])
            op.parameters[param] = str(parameters[param])
            # op.parameters[]
        #m.mapfield[5] = 10
        self.operators.append(op)
        return op

    # Receive a chain of operators, separate them in Wayang Operators
    # Compacts several Python executable operators in one Map Partition Wayang Operator
    def process_pipeline(self, stage):

        nested_udf = None
        nested_id = ""
        nested_predecessors = None
        nested_successors = None
        for node in reversed(stage):
            logging.debug(node.operator_type + " executable: " + str(node.python_exec) + " id: " + str(node.id))

            if not node.python_exec:
                if nested_udf is not None:

                    # Predecessors depends on last operator
                    # Successors depends on first operator
                    op = self.add_operator(nested_id, "map_partition", nested_udf)

                    ids = str(nested_id).split(",")
                    for id in ids:
                        self.operator_references[str(id)] = op

                    self.boundaries[str(nested_id)] = {}
                    self.boundaries[str(nested_id)]["end"] = nested_successors
                    self.boundaries[str(nested_id)]["start"] = nested_predecessors

                    nested_udf = None
                    nested_id = ""
                    nested_predecessors = None
                    nested_successors = None

                if node.operator.source:
                    op = self.add_source(node.id, node.operator_type, node.operator.udf)
                    self.operator_references[str(node.id)] = op
                    self.boundaries[str(node.id)] = {}
                    self.boundaries[str(node.id)]["end"] = node.successors.keys()

                elif node.operator.sink:
                    op = self.add_sink(node.id, node.operator_type, node.operator.udf)
                    self.operator_references[str(node.id)] = op
                    self.boundaries[str(node.id)] = {}
                    self.boundaries[str(node.id)]["start"] = node.predecessors.keys()

                # Regular operator to be processed in Java
                # Notice that those could include more parameters for Java
                else:
                    op = self.add_java_operator(node.id, node.operator_type, node.operator.udf, node.operator.parameters)
                    self.operator_references[str(node.id)] = op
                    self.boundaries[str(node.id)] = {}
                    self.boundaries[str(node.id)]["start"] = node.predecessors.keys()
                    self.boundaries[str(node.id)]["end"] = node.successors.keys()

            else:

                if nested_udf is None:
                    nested_udf = node.operator.udf
                    nested_id = node.id
                    # It is the last operator to execute in the map partition
                    nested_successors = node.successors.keys()

                else:
                    nested_udf = self.concatenate(nested_udf, node.operator.udf)
                    nested_id = str(node.id) + "," + str(nested_id)

                # Every iteration assign the first known predecessors
                nested_predecessors = node.predecessors.keys()

        # Just in case in the future some pipelines start with Python operators
        if nested_udf is not None:
            self.add_operator(nested_id, "map_partition", nested_udf)

            ids = nested_id.split(",")
            for id in ids:
                self.operator_references[id] = op

            self.boundaries[nested_id] = {}
            self.boundaries[nested_id]["end"] = nested_successors
            self.boundaries[nested_id]["start"] = nested_predecessors

    def __init__(self):
        pass

    # Takes 2 Functions and compact them in only one function
    @staticmethod
    def concatenate(function_a, function_b):
        def executable(iterable):
            return function_a(function_b(iterable))

        return executable

    # Set dependencies over final Wayang Operators
    def set_dependencies(self):

        for source in self.sources:

            if 'end' in self.boundaries[source.id]:
                op_successors = []
                for op_id in self.boundaries[source.id]['end']:
                    op_successors.append(str(self.operator_references[str(op_id)].id))
                source.successors.extend(op_successors)

        for sink in self.sinks:
            if 'start' in self.boundaries[sink.id]:
                op_predecessors = []
                for op_id in self.boundaries[sink.id]['start']:
                    op_predecessors.append(str(self.operator_references[str(op_id)].id))
                sink.predecessors.extend(op_predecessors)

        for op in self.operators:
            if 'start' in self.boundaries[op.id]:
                op_predecessors = []
                for op_id in self.boundaries[op.id]['start']:
                    op_predecessors.append(str(self.operator_references[str(op_id)].id))
                op.predecessors.extend(op_predecessors)

            if 'end' in self.boundaries[op.id]:
                op_successors = []
                for op_id in self.boundaries[op.id]['end']:
                    op_successors.append(str(self.operator_references[str(op_id)].id))
                op.successors.extend(op_successors)

    # Writes the message to a local directory
    def write_message(self, descriptor):

        finalpath = "../../protobuf/wayang_message"
        plan_configuration = pwb.WayangPlanProto()

        try:
            f = open(finalpath, "rb")
            plan_configuration.ParseFromString(f.read())
            f.close()
        except IOError:
            logging.warn("File " + finalpath + " did not exist. System generated a new file")

        plan = pwb.PlanProto()
        plan.sources.extend(self.sources)
        plan.operators.extend(self.operators)
        plan.sinks.extend(self.sinks)
        plan.input = pwb.PlanProto.string
        plan.output = pwb.PlanProto.string

        ctx = pwb.ContextProto()
        # ctx.platforms.extend([pwb.ContextProto.PlatformProto.java])
        for plug in descriptor.plugins:
            ctx.platforms.append(plug.value)
        # ctx.platforms.extend(descriptor.get_plugins())

        plan_configuration.plan.CopyFrom(plan)
        plan_configuration.context.CopyFrom(ctx)

        f = open(finalpath, "wb")
        f.write(plan_configuration.SerializeToString())
        f.close()
        pass

    # Send message as bytes to the Wayang Rest API
    def send_message(self, descriptor):

        plan_configuration = pwb.WayangPlanProto()

        plan = pwb.PlanProto()
        plan.sources.extend(self.sources)
        plan.operators.extend(self.operators)
        plan.sinks.extend(self.sinks)
        plan.input = pwb.PlanProto.string
        plan.output = pwb.PlanProto.string

        ctx = pwb.ContextProto()
        # ctx.platforms.extend([pwb.ContextProto.PlatformProto.java])
        for plug in descriptor.plugins:
            ctx.platforms.append(plug.value)
        # ctx.platforms.extend(descriptor.get_plugins())

        plan_configuration.plan.CopyFrom(plan)
        plan_configuration.context.CopyFrom(ctx)

        print("plan!")
        print(plan_configuration)

        msg_bytes = plan_configuration.SerializeToString()
        msg_64 = base64.b64encode(msg_bytes)

        logging.debug(msg_bytes)
        # response = requests.get("http://localhost:8080/plan/create/fromfile")
        data = {
            'message': msg_64
        }
        response = requests.post("http://localhost:8080/plan/create", data)
        logging.debug(response)
        # f = open(finalpath, "wb")
        # f.write(plan_configuration.SerializeToString())
        # f.close()
        pass
