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
import pkg_resources

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logger = logging.getLogger(__name__)


def if_environ_file(key: str, default_value: str) -> str:
    return os.environ[key] if key in os.environ else os.path.abspath(default_value)


def if_environ_int(key: str, default_value: int) -> int:
    return int(os.environ[key]) if key in os.environ else default_value


BASE_DIR = pkg_resources.resource_filename("pywy", "")
RC_DIR = if_environ_file("PYWY_RC_HOME", os.path.expanduser("~/.pywy"))
RC_TEST_DIR = if_environ_file("PYWY_RC_TEST_HOME", "{}/tests/resources".format(BASE_DIR))
# RC_TEST_OUT_DIR = if_environ("PYWY_RC_TEST_OUT_HOME", "{}/../../output".format(BASE_DIR))
RC_BENCHMARK_SIZE = if_environ_int("PYWY_RC_BENCHMARK_SIZE", 2)


logger.info(" Environment variables")
logger.info(" ############################")
logger.info(f" ## {BASE_DIR=}")
logger.info(f" ## {RC_DIR=}")
logger.info(f" ## {RC_TEST_DIR=}")
# logger.info(f" ## {RC_TEST_OUT_DIR=}")
logger.info(f" ## {RC_BENCHMARK_SIZE=}")
logger.info(" ############################")
# print(HOME_DIR)
