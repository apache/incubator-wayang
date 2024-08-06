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
from pywy.dataquanta import WayangContext, Configuration
from unittest.mock import Mock
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

def fm_func(w: str) -> Iterable[str]:
    return w.split()

def filter_func(w: str) -> bool:
    return w.strip() != ""

def map_func(w: str) -> (str, int):
    return (w.lower(), 1)

def key_func(t: (str, int)) -> str:
    return t[0]

def reduce_func(t1: (str, int), t2: (str, int)) -> (str, int):
    return (t1[0], int(t1[1]) + int(t2[1]))

def sort_func(tup: (str, int)):
    return tup[0]

class TestWCPlanToJson(unittest.TestCase):
    def test_to_json(self):
        # anonymous functions with type hints
        ctx = WayangContext() \
            .register({JavaPlugin, SparkPlugin}) \
            .textfile("file:///var/www/html/README.md") \
            .flatmap(lambda w: w.split(), str, str) \
            .filter(lambda w: w.strip() != "", str) \
            .map(lambda w: (w.lower(), 1), str, (str, int)) \
            .reduce_by_key(lambda t: t[0], lambda t1, t2: (t1[0], int(t1[1]) + int(t2[1])), (str, int)) \
            .store_textfile("file:///var/www/html/data/wordcount-out-python.txt", (str, int))
        self.assertEqual(True, True)

        config = Configuration()
        config.set_property("wayang.api.python.worker", "/var/www/html/python/src/pywy/execution/worker.py")

        # named functions with  signatures
        ctx = WayangContext(config) \
            .register({JavaPlugin, SparkPlugin}) \
            .textfile("file:///var/www/html/README.md") \
            .flatmap(fm_func) \
            .filter(filter_func) \
            .map(map_func) \
            .reduce_by_key(key_func, reduce_func) \
            .sort(sort_func) \
            .store_textfile("file:///var/www/html/data/wordcount-out-python.txt", (str, int))
        self.assertEqual(True, True)

if __name__ == "__main__":
    unittest.main()
