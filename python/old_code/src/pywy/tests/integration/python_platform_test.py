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

import logging
import os
import unittest
import tempfile
from itertools import chain
from typing import List, Iterable

from pywy.config import RC_TEST_DIR as ROOT
from pywy.dataquanta import WayangContext
from pywy.plugins import PYTHON

logger = logging.getLogger(__name__)


class TestIntegrationPythonPlatform(unittest.TestCase):

    file_10e0: str

    def setUp(self):
        self.file_10e0 = "{}/10e0MB.input".format(ROOT)

    @staticmethod
    def seed_small_grep(validation_file):
        def pre(a: str) -> bool:
            return 'six' in a

        fd, path_tmp = tempfile.mkstemp()

        dq = WayangContext() \
            .register(PYTHON) \
            .textfile(validation_file) \
            .filter(pre)

        return dq, path_tmp, pre

    def validate_files(self,
                       validation_file,
                       outputed_file,
                       read_and_convert_validation,
                       read_and_convert_outputed,
                       delete_outputed=True,
                       print_variable=False):
        lines_filter: List[int]
        with open(validation_file, 'r') as f:
            lines_filter = list(read_and_convert_validation(f))
            selectivity = len(lines_filter)

        lines_platform: List[int]
        with open(outputed_file, 'r') as fp:
            lines_platform = list(read_and_convert_outputed(fp))
            elements = len(lines_platform)

        if delete_outputed:
            os.remove(outputed_file)

        if print_variable:
            logger.info(f"{lines_platform=}")
            logger.info(f"{lines_filter=}")
            logger.info(f"{elements=}")
            logger.info(f"{selectivity=}")

        self.assertEqual(selectivity, elements)
        self.assertEqual(lines_filter, lines_platform)

    def test_grep(self):

        dq, path_tmp, pre = self.seed_small_grep(self.file_10e0)

        dq.store_textfile(path_tmp)

        def convert_validation(file):
            return filter(pre, file.readlines())

        def convert_outputed(file):
            return file.readlines()

        self.validate_files(
            self.file_10e0,
            path_tmp,
            convert_validation,
            convert_outputed
        )

    def test_dummy_map(self):

        def convert(a: str) -> int:
            return len(a)

        dq, path_tmp, pre = self.seed_small_grep(self.file_10e0)

        dq.map(convert) \
            .store_textfile(path_tmp)

        def convert_validation(file):
            return map(convert, filter(pre, file.readlines()))

        def convert_outputed(file):
            return map(lambda x: int(x), file.read().splitlines())

        self.validate_files(
            self.file_10e0,
            path_tmp,
            convert_validation,
            convert_outputed
        )

    def test_dummy_flatmap(self):
        def fm_func(string: str) -> Iterable[str]:
            return string.strip().split(" ")

        dq, path_tmp, pre = self.seed_small_grep(self.file_10e0)

        dq.flatmap(fm_func) \
            .store_textfile(path_tmp, '\n')

        def convert_validation(file):
            return chain.from_iterable(map(fm_func, filter(pre, file.readlines())))

        def convert_outputed(file):
            return file.read().splitlines()

        self.validate_files(
            self.file_10e0,
            path_tmp,
            convert_validation,
            convert_outputed
        )
