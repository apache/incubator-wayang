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

import inspect
import unittest
from unittest.mock import Mock

from pywy.exception import PywyException
from pywy.types import get_type_function, get_type_bifunction, get_type_flatmap_function, get_type_predicate

empty_type = inspect._empty


class TestUnitTypesPredicate(unittest.TestCase):
    def setUp(self):
        pass

    def test_predicate_without_parameters(self):
        def pred() -> bool:
            return True

        try:
            get_type_predicate(pred)
            self.fail("The predicates parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the Predicate are distinct than one," in str(ex))

    def test_predicate_with_one_parameter_no_type(self):
        def pred(x) -> bool:
            return True

        try:
            pred_type = get_type_predicate(pred)
            self.assertEqual(pred_type, empty_type)
        except PywyException as ex:
            self.fail(str(ex))

    def test_predicate_with_one_parameter_with_basic_type(self):
        def pred(x: int) -> bool:
            return True

        try:
            pred_type = get_type_predicate(pred)
            self.assertEqual(pred_type, int)
        except PywyException as ex:
            self.fail(str(ex))

    def test_predicate_with_one_parameter_with_obe_type(self):
        def pred(x: Mock) -> bool:
            return True

        try:
            pred_type = get_type_predicate(pred)
            self.assertEqual(pred_type, Mock)
        except PywyException as ex:
            self.fail(str(ex))

    def test_predicate_with_two_parameters(self):
        def pred(x: Mock, y: Mock) -> bool:
            return True

        try:
            get_type_predicate(pred)
            self.fail("the predicate can have just one input")
        except PywyException as ex:
            self.assertTrue("the parameters for the Predicate are distinct than one" in str(ex))


class TestUnitTypesFunction(unittest.TestCase):
    def setUp(self):
        pass

    def test_function_without_parameters_no_return(self):
        def func():
            return

        try:
            get_type_function(func)
            self.fail("The function parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one," in str(ex))

    def test_function_with_one_parameter_no_type_no_return(self):
        def func(x):
            return

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, empty_type)
            self.assertEqual(output_type, empty_type)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_basic_type_no_return(self):
        def func(x: int):
            return

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, int)
            self.assertEqual(output_type, empty_type)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_obj_type_no_return(self):
        def func(x: Mock):
            return

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, Mock)
            self.assertEqual(output_type, empty_type)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_two_parameters_no_return(self):
        def func(x: Mock, y: Mock):
            return

        try:
            get_type_function(func)
            self.fail("the function can have just one input")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one" in str(ex))

    def test_function_without_parameters_basic_return(self):
        def func() -> int:
            return 0

        try:
            get_type_function(func)
            self.fail("The function parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one," in str(ex))

    def test_function_with_one_parameter_no_type_basic_return(self):
        def func(x) -> int:
            return 0

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, empty_type)
            self.assertEqual(output_type, int)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_basic_type_basic_return(self):
        def func(x: int) -> int:
            return 0

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, int)
            self.assertEqual(output_type, int)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_obj_type_basic_return(self):
        def func(x: Mock) -> int:
            return 0

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, Mock)
            self.assertEqual(output_type, int)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_two_parameters_basic_return(self):
        def func(x: Mock, y: Mock) -> int:
            return 0

        try:
            get_type_function(func)
            self.fail("the function can have just one input")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one" in str(ex))

    def test_function_without_parameters_obj_return(self):
        def func() -> Mock:
            return Mock()

        try:
            get_type_function(func)
            self.fail("The function parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one," in str(ex))

    def test_function_with_one_parameter_no_type_obj_return(self):
        def func(x) -> Mock:
            return Mock()

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, empty_type)
            self.assertEqual(output_type, Mock)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_basic_type_basic_return(self):
        def func(x: int) -> Mock:
            return Mock()

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, int)
            self.assertEqual(output_type, Mock)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_one_parameter_with_obe_type_basic_return(self):
        def func(x: Mock) -> Mock:
            return Mock()

        try:
            input_type, output_type = get_type_function(func)
            self.assertEqual(input_type, Mock)
            self.assertEqual(output_type, Mock)
        except PywyException as ex:
            self.fail(str(ex))

    def test_function_with_two_parameters_basic_return(self):
        def func(x: Mock, y: Mock) -> Mock:
            return Mock()

        try:
            get_type_function(func)
            self.fail("the function can have just one input")
        except PywyException as ex:
            self.assertTrue("the parameters for the Function are distinct than one" in str(ex))


class TestUnitTypesBiFunction(unittest.TestCase):
    def setUp(self):
        pass

    # TODO add the missing test for get_type_bifunction
    def test_bifunction_without_parameters_no_return(self):
        def func():
            return

        try:
            get_type_bifunction(func)
            self.fail("The bifunction parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the BiFunction are distinct than two," in str(ex))


class TestUnitTypesFlatmapFunction(unittest.TestCase):
    def setUp(self):
        pass

    # TODO add the missing test for get_type_flatmap_function
    def test_flatmapfunction_without_parameters_no_return(self):
        def func():
            return

        try:
            get_type_flatmap_function(func)
            self.fail("The bifunction parameters are mandatory")
        except PywyException as ex:
            self.assertTrue("the parameters for the FlatmapFunction are distinct than one," in str(ex))
