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

import unittest
from typing import Tuple, Callable, Iterable, List
from pywy.dataquanta import WayangContext
from unittest.mock import Mock
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin
from pywy.basic.model.ops import Mean, Cast, Eq, ArgMax, Input, Op, CrossEntropyLoss, Linear, Sigmoid
from pywy.basic.model.optimizer import GradientDescent
from pywy.basic.model.option import Option
from pywy.basic.model.models import DLModel


class TestWCPlanToJson(unittest.TestCase):
    def test_to_json(self):
        # anonymous functions with type hints
        l1 = Linear(4, 64, True)
        s1 = Sigmoid()
        l2 = Linear(64, 3, True)

        s1.with_ops(l1.with_ops(Input(Input.Type.FEATURES)))
        l2.with_ops(s1)

        model = DLModel(l2)

        criterion = CrossEntropyLoss(3)
        acc = Mean(0)
        acc.with_ops(
            Cast(Op.DType.FLOAT32).with_ops(
                Eq().with_ops(
                    ArgMax(1).with_ops(
                        Input(Input.Type.PREDICTED)
                    ),
                    Input(Input.Type.LABEL, Op.DType.INT32)
                )
            )
        )

        optimizer = GradientDescent(0.02)
        option = Option(criterion, optimizer, 6, 100)

        floats: List[List[float]] = [[5.1, 3.5, 1.4, 0.2]]

        ints: List[List[int]] = [[0, 0, 1, 1, 2, 2]]

        ctx = WayangContext() \
            .register({JavaPlugin, SparkPlugin})
        left = ctx.textfile("file:///var/www/html/README.md").map(lambda x: floats, str, List[List[float]])
        right = ctx.textfile("file:///var/www/html/README.md").map(lambda x: ints, str, List[List[int]])

        left.dlTraining(model, option, right, List[float], List[int]) \
            .predict(right, List[float], List[float]) \
            .store_textfile("file:///var/www/html/data/wordcount-out-python.txt", Tuple[str, int])
        self.assertEqual(True, True)

if __name__ == "__main__":
    unittest.main()
