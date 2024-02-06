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
from typing import Set

import pywy.platforms.jvm.serializable.pywayangplan_pb2 as pwb
import os
import cloudpickle
import logging
import requests
import base64


# Writes Wayang Plan from several stages
from pywy.exception import PywyException
from pywy.operators import SinkOperator
from pywy.operators.source import SourceUnaryOperator
from pywy.operators.unary import UnaryToUnaryOperator
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMOperator, WayangJVMTextFileSink


class PlanWritter:

    def __init__(self):
        self.originals: Set[WayangJVMOperator] = set()

    def add_operator(self, operator: WayangJVMOperator):
        self.originals.add(operator)

    def add_proto_unary_operator(self, operator: WayangJVMOperator):
        op = pwb.OperatorProto()
        op.id = str(operator.name)
        op.type = operator.kind
        op.udf = cloudpickle.dumps(operator.udf)
        op.path = str(None)
        return op

    def add_proto_source_operator(self, operator: WayangJVMOperator):
        source = pwb.OperatorProto()
        source.id = str(operator.name)
        source.type = operator.kind
        source.path = os.path.abspath(operator.path)
        source.udf = chr(0).encode('utf-8')
        return source

    def add_proto_sink_operator(self, operator: WayangJVMOperator):
        sink = pwb.OperatorProto()
        sink.id = str(operator.name)
        sink.type = operator.kind
        sink.path = os.path.abspath(operator.path)
        sink.udf = chr(0).encode('utf-8')
        return sink

    def send_message_to_wayang(self):
        connections = {}
        sources = []
        sinks = []
        operators = []
        for operator in self.originals:
            if not operator.is_unary():
                raise PywyException(
                    "the not unary operator are not supported".format(
                        type(operator),
                        operator
                    )
                )
            if operator.is_operator():
                connections[operator] = self.add_proto_unary_operator(operator)
                operators.append(connections[operator])
            elif operator.is_source():
                connections[operator] = self.add_proto_source_operator(operator)
                sources.append(connections[operator])
            elif operator.is_sink():
                connections[operator] = self.add_proto_sink_operator(operator)
                sinks.append(connections[operator])
            else:
                raise PywyException(
                    "the type {} for the operator {} is not supported {}".format(
                        type(operator),
                        operator,
                        WayangJVMTextFileSink.mro()
                    )
                )
        for operator in self.originals:
            current = connections[operator]
            for ele in operator.previous:
                current.predecessors.append(connections.get(ele).id)
            for ele in operator.nexts:
                current.successors.append(connections.get(ele).id)

        plan_configuration = pwb.WayangPlanProto()

        plan = pwb.PlanProto()
        plan.sources.extend(sources)
        plan.operators.extend(operators)
        plan.sinks.extend(sinks)
        plan.input = pwb.PlanProto.string
        plan.output = pwb.PlanProto.string

        ctx = pwb.ContextProto()
        ctx.platforms.extend([pwb.ContextProto.PlatformProto.java])

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
