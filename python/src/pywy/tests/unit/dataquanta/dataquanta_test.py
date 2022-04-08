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
from typing import Tuple, Callable, Iterable
from unittest.mock import Mock

from pywy.dataquanta import WayangContext
from pywy.dataquanta import DataQuanta
from pywy.exception import PywyException
from pywy.operators import *


class TestUnitCoreTranslator(unittest.TestCase):
    context: WayangContext

    def setUp(self):
        self.context = Mock()
        pass

    def build_seed(self) -> Tuple[PywyOperator, DataQuanta]:
        operator = PywyOperator("Empty")
        dq = DataQuanta(self.context, operator)
        return operator, dq

    def test_create(self):
        (operator, dq) = self.build_seed()

        self.assertIsInstance(dq, DataQuanta)
        self.assertEqual(dq.context, self.context)
        self.assertEqual(dq.operator, operator)

    def test_connect(self):
        operator = PywyOperator("Empty1")
        operator2 = PywyOperator("Empty2")
        dq = DataQuanta(self.context, operator)

        self.assertIsNone(operator2.inputOperator[0])
        after_operator2 = dq._connect(operator2)
        self.assertEqual(operator2, after_operator2)
        self.assertIsNotNone(operator2.inputOperator[0])
        self.assertEqual(operator, operator2.inputOperator[0])
        self.assertEqual(operator.outputOperator[0], operator2)

    def validate_filter(self, filtered: DataQuanta, operator: PywyOperator):
        self.assertIsInstance(filtered, DataQuanta)
        self.assertIsInstance(filtered.operator, FilterOperator)
        self.assertEqual(filtered.context, self.context)
        self.assertNotEqual(filtered.operator, operator)
        self.assertEqual(filtered.operator.inputOperator[0], operator)

    def test_filter_lambda(self):
        (operator, dq) = self.build_seed()
        pred: Callable = lambda x: "" in x
        filtered = dq.filter(pred)
        self.validate_filter(filtered, operator)

    def test_filter_func(self):
        (operator, dq) = self.build_seed()

        def pred(x: str) -> bool:
            return "" in x

        filtered = dq.filter(pred)
        self.validate_filter(filtered, operator)

    def test_filter_func_lambda(self):
        (operator, dq) = self.build_seed()

        def pred(x):
            return "" in x

        filtered = dq.filter(lambda x: pred(x))
        self.validate_filter(filtered, operator)

    def validate_map(self, mapped: DataQuanta, operator: PywyOperator):
        self.assertIsInstance(mapped, DataQuanta)
        self.assertIsInstance(mapped.operator, MapOperator)
        self.assertEqual(mapped.context, self.context)
        self.assertNotEqual(mapped.operator, operator)
        self.assertEqual(mapped.operator.inputOperator[0], operator)

    def test_map_lambda(self):
        (operator, dq) = self.build_seed()
        func: Callable = lambda x: len(x)
        mapped = dq.map(func)
        self.validate_map(mapped, operator)

    def test_map_func(self):
        (operator, dq) = self.build_seed()

        def func(x: str) -> int:
            return len(x)

        mapped = dq.map(func)
        self.validate_map(mapped, operator)

    def test_map_func_lambda(self):
        (operator, dq) = self.build_seed()

        def func(x):
            return x == 0

        mapped = dq.map(lambda x: func(x))
        self.validate_map(mapped, operator)

    def validate_flatmap(self, flatted: DataQuanta, operator: PywyOperator):
        self.assertIsInstance(flatted, DataQuanta)
        self.assertIsInstance(flatted.operator, FlatmapOperator)
        self.assertEqual(flatted.context, self.context)
        self.assertNotEqual(flatted.operator, operator)
        self.assertEqual(flatted.operator.inputOperator[0], operator)

    def test_flatmap_lambda(self):
        (operator, dq) = self.build_seed()
        func: Callable = lambda x: x.split(" ")
        try:
            flatted = dq.flatmap(func)
            self.validate_flatmap(flatted, operator)
        except PywyException as e:
            self.assertTrue("the return for the FlatmapFunction is not Iterable" in str(e))

    def test_flatmap_func(self):
        (operator, dq) = self.build_seed()

        def fmfunc(i: str) -> Iterable[str]:
            for x in range(len(i)):
                yield str(x)

        flatted = dq.flatmap(fmfunc)
        self.validate_flatmap(flatted, operator)

    def test_flatmap_func_lambda(self):
        (operator, dq) = self.build_seed()

        try:
            fm_func_lambda: Callable[[str], Iterable[str]] = lambda i: [str(x) for x in range(len(i))]
            flatted = dq.flatmap(fm_func_lambda)
            self.assertRaises("the current implementation does not support lambdas")
            # self.validate_flatmap(flatted, operator)
        except PywyException as e:
            self.assertTrue("the return for the FlatmapFunction is not Iterable" in str(e))
