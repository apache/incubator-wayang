from typing import List

from pywy.graph.graphtypes import WGraphOfOperator, NodeOperator
from pywy.core import Channel
from pywy.core import Executor
from pywy.core import PywyPlan
from pywy.platforms.python.operators.PyExecutionOperator import PyExecutionOperator


class PyExecutor(Executor):

    def __init__(self):
        super(PyExecutor, self).__init__()

    def execute(self, plan):
        pywyPlan: PywyPlan = plan
        graph = WGraphOfOperator(pywyPlan.sinks)

        def exec(current: NodeOperator, next: NodeOperator):
            if current is None:
                return

            py_current: PyExecutionOperator = current.current
            if py_current.outputs == 0:
                py_current.execute(py_current.inputChannel, [])
                return

            if next is None:
                return
            py_next: PyExecutionOperator = next.current
            outputs = py_current.get_output_channeldescriptors()
            inputs = py_next.get_input_channeldescriptors()

            intersect = outputs.intersection(inputs)
            if len(intersect) == 0:
                raise Exception(
                    "The operator(A) {} can't connect with (B) {}, because the output of (A) is {} and the input of (B) is {}"
                     .format(
                        py_current,
                        py_next,
                        outputs,
                        inputs
                    )
                )
            if len(intersect) > 1:
                raise Exception(
                    "The interaction between the operator (A) {} and (B) {}, can't be decided because are several channel availables {}"
                     .format(
                        py_current,
                        py_next,
                        intersect
                    )
                )
            #TODO validate if is valite for several output
            py_current.outputChannel: List[Channel] = [intersect.pop().create_instance()]

            py_current.execute(py_current.inputChannel, py_current.outputChannel)

            py_next.inputChannel = py_current.outputChannel


        graph.traversal(None, graph.starting_nodes, exec)