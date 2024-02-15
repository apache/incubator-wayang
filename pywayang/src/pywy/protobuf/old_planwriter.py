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

import pywy.protobuf.pywayangplan_pb2 as pwb
import os
import pickle
import struct
import base64


class OldMessageWriter:

    def __init__(self, descriptor):

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
                """base64_bytes = base64.b64encode(pre.udf)
                pre.udf = base64_bytes"""

                """pre.serialize_udf()
                print("pre.udf")
                print(pre.udf)
                func = pickle.loads(pre.udf)
                print("func")
                print(func)
                middle_operators.append(pre)

                # Testing
                msg = pre.udf
                base64_bytes = base64.b64encode(msg)
                base64_message = base64.b64decode(base64_bytes)
                func2 = pickle.loads(base64_message)
                print(base64_message)
                func3 = pickle.loads(b'\x80\x04\x955\x04\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\r_builtin_type\x94\x93\x94\x8c\nLambdaType\x94\x85\x94R\x94(h\x02\x8c\x08CodeType\x94\x85\x94R\x94(K\x01K\x00K\x01K\x03K\x13C\nt\x00\x88\x00|\x00\x83\x02S\x00\x94N\x85\x94\x8c\x06filter\x94\x85\x94\x8c\x08iterator\x94\x85\x94\x8cS/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/dataquanta.py\x94\x8c\x04func\x94K%C\x02\x00\x01\x94\x8c\x03udf\x94\x85\x94)t\x94R\x94}\x94(\x8c\x0b__package__\x94\x8c\x0corchestrator\x94\x8c\x08__name__\x94\x8c\x17orchestrator.dataquanta\x94\x8c\x08__file__\x94\x8cS/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/dataquanta.py\x94uNNh\x00\x8c\x10_make_empty_cell\x94\x93\x94)R\x94\x85\x94t\x94R\x94\x8c\x1ccloudpickle.cloudpickle_fast\x94\x8c\x12_function_setstate\x94\x93\x94h"}\x94}\x94(h\x19h\x10\x8c\x0c__qualname__\x94\x8c\x1fDataQuanta.filter.<locals>.func\x94\x8c\x0f__annotations__\x94}\x94\x8c\x0e__kwdefaults__\x94N\x8c\x0c__defaults__\x94N\x8c\n__module__\x94h\x1a\x8c\x07__doc__\x94N\x8c\x0b__closure__\x94h\x00\x8c\n_make_cell\x94\x93\x94h\x05(h\x08(K\x01K\x00K\x01K\x02KSC\x10t\x00|\x00\x83\x01d\x01\x16\x00d\x02k\x03S\x00\x94NK\x02K\x00\x87\x94\x8c\x03int\x94\x85\x94\x8c\x04elem\x94\x85\x94\x8cM/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/main.py\x94\x8c\x08<lambda>\x94K\x18C\x00\x94))t\x94R\x94}\x94(h\x17Nh\x19\x8c\x08__main__\x94h\x1b\x8cM/Users/rodrigopardomeza/wayang/incubator-wayang/pywayang/orchestrator/main.py\x94uNNNt\x94R\x94h%hB}\x94}\x94(h\x19h:h(\x8c\x1dplan_filter.<locals>.<lambda>\x94h*}\x94h,Nh-Nh.h?h/Nh0N\x8c\x17_cloudpickle_submodules\x94]\x94\x8c\x0b__globals__\x94}\x94u\x86\x94\x86R0\x85\x94R\x94\x85\x94hG]\x94hI}\x94u\x86\x94\x86R0.')
                for i in func3([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]):
                    print(i)"""
            op = pre

        """for mid in middle_operators:
            print(mid.operator_type)
            print(pickle.loads(mid.udf))
            func = pickle.loads(mid.udf)
            for i in func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]):
                print(i)"""

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

class func_inteface:

    def __init__(self, node, nested_udf):
        self.node = node
        self.nested_udf = nested_udf

    def func(self, iterable):
        return self.node.operator.udf(self.nested_udf(iterable))


class MessageWriter:
    sources = []
    operators = []
    sinks = []

    def add_source(self, operator_id, operator_type, path, predecessors, successors):
        source = pwb.OperatorProto()
        source.id = operator_id
        source.type = operator_type
        source.path = os.path.abspath(path)
        source.udf = None
        source.predecessors = predecessors
        source.successors = successors
        self.sources.append(source)

    def add_sink(self, operator_id, operator_type, path, predecessors, successors):
        sink = pwb.OperatorProto()
        sink.id = operator_id
        sink.type = operator_type
        sink.path = os.path.abspath(path)
        sink.udf = None
        sink.predecessors = predecessors
        sink.successors = successors
        self.sinks.append(sink)

    def add_operator(self, operator_id, operator_type, udf, path, predecessors, successors):
        op = pwb.OperatorProto()
        op.id = operator_id
        op.type = operator_type
        op.udf = udf
        op.path = path
        op.predecessors = predecessors
        op.successors = successors
        self.operators.append(op)

    def process_pipeline(self, stage):

        nested_udf = None
        nested_id = ""
        for node in reversed(stage):
            print("########")
            print(node.operator_type, "executable:", node.python_exec, "id:", node.id)

            if nested_udf is not None:
                print("review pre")
                print( nested_udf)
                print( nested_udf(["Wilo","lifo","Wifo"]))

            if not node.python_exec:
                if nested_udf is not None:
                    """self.add_operator(nested_id, "map_partition", nested_udf, None
                                      # obtain predecessors and successors
                                      , successors=[node.id]
                                      )"""
                    print("node", nested_id)
                    print(nested_udf)
                    print("he muerto")
                    print( nested_udf(["Wilo","lifo","Wifo"]))

                    t = nested_udf(["Wilo","lifo","Wifo"])
                    print("jajajarvard")
                    print(t)
                    for i in t:
                        print(i)
                    nested_udf = None
                    nested_id = ""

                """if node.operator.source:
                    self.add_source(
                        node.id, node.operator_type, node.operator.udf,
                        node.predecessors, node.operator.successor)
                else:
                    self.add_operator(
                        node.id, node.operator_type, None, node.operator.udf,
                        node.predecessors, node.operator.successor)"""
            else:
                print("adding", node.id)
                if nested_udf is None:
                    nested_udf = node.operator.udf
                    nested_id = node.id
                else:
                    print("paseeeeeee viste")
                    tmp = nested_udf

                    print( tmp(["Wilo","lifo","Wifo"]))

                    #def func(_, iterable):
                    #    return nested_udf(node.operator.udf(iterable))
                    nested_udf = self.concatenate(nested_udf, node.operator.udf)
                    print( nested_udf(["Wilo","lifo","Wifo"]))
                    print(nested_udf)

                    # nested_udf = func_inteface(node, nested_udf)
                    nested_id = str(node.id) + "," + str(nested_id)

            if nested_udf is not None:
                print("review")
                print( nested_udf)
                print( nested_udf(["Wilo","lifo","Wifo"]))

        if nested_udf is not None:
            """self.add_operator(nested_id, "map_partition", nested_udf, None
                              # obtain predecessors and successors
                              , successors=[node.id]
                              )"""
            print("node", nested_id)
            print(nested_udf)
            t = nested_udf(["Wilo","lifo","Wifo"])
            print("jajajarvard2")
            print(t)
            for i in t:
                print(i)
            nested_udf = None
            nested_id = ""

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
