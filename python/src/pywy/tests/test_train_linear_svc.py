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
from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

class TestTrainLinearSVC(unittest.TestCase):

    def test_train_and_predict(self):
        ctx = WayangContext().register({JavaPlugin, SparkPlugin})

        features = ctx.load_collection([[0.0, 1.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]])
        labels = ctx.load_collection([1.0, 1.0, 0.0, 0.0])

        model = features.train_linear_svc(labels, max_iter=10, reg_param=0.1)
        predictions = model.predict(features)

        result = predictions.collect()
        print("Predictions:", result)

        self.assertEqual(len(result), 4)
        for pred in result:
            self.assertIn(pred, [0.0, 1.0])

if __name__ == "__main__":
    unittest.main()
