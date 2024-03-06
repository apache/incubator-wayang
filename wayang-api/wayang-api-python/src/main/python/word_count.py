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
import tempfile
import logging
import os
import sys
from typing import List, Iterable

from pywy.dataquanta import WayangContext
from pywy.plugins import JVMs

def test_word_count(file_path):
    def fm_func(string: str) -> Iterable[str]:
        return string.split("|")

    WayangContext() \
        .register(JVMs) \
        .textfile(file_path) \
        .filter(lambda elem: len(str(elem).split("|")[0]) < 4) \
        .flatmap(fm_func) \
        .store_textfile("../resources/output.txt")

if __name__ == '__main__':
    file_path = sys.argv[1]
    test_word_count(file_path)
