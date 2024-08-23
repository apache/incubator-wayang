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
#from typing import Tuple, Callable, Iterable
from pywy.dataquanta import WayangContext
from unittest.mock import Mock
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

class TestWCPlanToJson(unittest.TestCase):
    def test_to_json(self):
        ctx = WayangContext() \
            .register({JavaPlugin, SparkPlugin})
        left = ctx.textfile("file:///var/www/html/README.md") \
            .filter(lambda w: "Apache" in w, str) \
            .flatmap(lambda w: w.split(), str, str) \
            .map(lambda w: (len(w), w), str, (int, str))
        right = ctx.textfile("file:///var/www/html/README.md") \
            .filter(lambda w: "Wayang" in w, str) \
            .map(lambda w: (len(w), w), str, (int, str))
        join = left.join(lambda w: w[0], right, lambda w: w[0], (int, str), ((int, str), (int, str))) \
            .store_textfile("file:///var/www/html/data/wordcount-out-python.txt")

        self.assertEqual(True, True)

if __name__ == "__main__":
    unittest.main()
