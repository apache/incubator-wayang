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

from pywy.core import Executor, ChannelDescriptor
from pywy.core import PywyPlan
from pywy.platforms.jvm.channels import DISPATCHABLE_CHANNEL_DESCRIPTOR
from pywy.platforms.jvm.graph import NodeDispatch, WGraphDispatch
from pywy.platforms.jvm.operator import JVMExecutionOperator
from pywy.platforms.jvm.serializable.wayang_jvm_operator import WayangJVMOperator


class JVMExecutor(Executor):

    def __init__(self):
        super(JVMExecutor, self).__init__()

    def execute(self, plan):
        pywyPlan: PywyPlan = plan
        graph = WGraphDispatch(pywyPlan.sinks)

        # TODO get this information by a configuration and ideally by the context
        descriptor_default: ChannelDescriptor = DISPATCHABLE_CHANNEL_DESCRIPTOR

        def execute(op_current: NodeDispatch, op_next: NodeDispatch):
            if op_current is None:
                return

            jvm_current: JVMExecutionOperator = op_current.current
            if jvm_current.outputs == 0:
                jvm_current.execute(jvm_current.inputChannel, [])
                return

            if op_next is None:
                return

            jvm_next: JVMExecutionOperator = op_next.current
            outputs = jvm_current.get_output_channeldescriptors()
            inputs = jvm_next.get_input_channeldescriptors()

            intersect = outputs.intersection(inputs)
            if len(intersect) == 0:
                raise Exception(
                    "The operator(A) {} can't connect with (B) {}, "
                    "because the output of (A) is {} and the input of (B) is {} ".format(
                        jvm_current,
                        jvm_next,
                        outputs,
                        inputs
                    )
                )

            if len(intersect) > 1:
                if descriptor_default is None:
                    raise Exception(
                        "The interaction between the operator (A) {} and (B) {}, "
                        "can't be decided because are several channel availables {}".format(
                            jvm_current,
                            jvm_next,
                            intersect
                        )
                    )
                descriptor = descriptor_default
            else:
                descriptor = intersect.pop()

            # TODO validate if is valite for several output
            jvm_current.outputChannel[0] = descriptor.create_instance()

            jvm_current.execute(jvm_current.inputChannel, jvm_current.outputChannel)

            jvm_next.inputChannel = jvm_current.outputChannel

        graph.traversal(graph.starting_nodes, execute)

        magic: JVMExecutionOperator = graph.starting_nodes[0].current

        magic.translate_context.generate_request()


