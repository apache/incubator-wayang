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
from pywy.plugins import JVMs

logger = logging.getLogger(__name__)

input_file = "../resources/text.input"

def seed_small_grep(validation_file):
    def pre(a: str) -> bool:
        return 'six' in a

    fd, path_tmp = tempfile.mkstemp()

    dq = WayangContext() \
        .register(JVMs) \
        .textfile(validation_file) \
        .filter(pre)

    return dq, path_tmp, pre

def validate_files(validation_file,
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

def test_grep():
    dq, path_tmp, pre = seed_small_grep(input_file)

    dq.store_textfile(path_tmp)

    def convert_validation(file):
        return filter(pre, file.readlines())

    def convert_outputed(file):
        return file.readlines()

    validate_files(
        input_file,
        path_tmp,
        convert_validation,
        convert_outputed
    )

if __name__ == '__main__':
    test_grep()
