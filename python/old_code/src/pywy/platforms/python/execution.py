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

from pywy.graph.types import WGraphOfOperator, NodeOperator
from pywy.core import ChannelDescriptor
from pywy.core import Executor
from pywy.core import PywyPlan
from pywy.operators import TextFileSource
from pywy.platforms.python.channels import PY_ITERATOR_CHANNEL_DESCRIPTOR
from pywy.platforms.python.operator.py_execution_operator import PyExecutionOperator


class PyExecutor(Executor):

    def __init__(self):
        super(PyExecutor, self).__init__()

    def execute(self, plan):
        pywyPlan: PywyPlan = plan
        graph = WGraphOfOperator(pywyPlan.sinks)

        # TODO get this information by a configuration and ideally by the context
        descriptor_default: ChannelDescriptor = PY_ITERATOR_CHANNEL_DESCRIPTOR
        files_pool = []

        def execute(op_current: NodeOperator, op_next: NodeOperator):
            if op_current is None:
                return

            py_current: PyExecutionOperator = op_current.current
            if py_current.outputs == 0:
                py_current.execute(py_current.inputChannel, [])
                return

            if op_next is None:
                return
            py_next: PyExecutionOperator = op_next.current
            outputs = py_current.get_output_channeldescriptors()
            inputs = py_next.get_input_channeldescriptors()

            intersect = outputs.intersection(inputs)
            if len(intersect) == 0:
                raise Exception(
                    "The operator(A) {} can't connect with (B) {}, "
                    "because the output of (A) is {} and the input of (B) is {} ".format(
                        py_current,
                        py_next,
                        outputs,
                        inputs
                    )
                )

            if len(intersect) > 1:
                if descriptor_default is None:
                    raise Exception(
                        "The interaction between the operator (A) {} and (B) {}, "
                        "can't be decided because are several channel availables {}".format(
                            py_current,
                            py_next,
                            intersect
                        )
                    )
                descriptor = descriptor_default
            else:
                descriptor = intersect.pop()

            # TODO validate if is valite for several output
            py_current.outputChannel[0] = descriptor.create_instance()

            py_current.execute(py_current.inputChannel, py_current.outputChannel)

            py_next.inputChannel = py_current.outputChannel

            if isinstance(py_current, TextFileSource):
                files_pool.append(py_current.outputChannel[0].provide_iterable())

        graph.traversal(graph.starting_nodes, execute)
        # close the files used during the execution
        for f in files_pool:
            f.close()
