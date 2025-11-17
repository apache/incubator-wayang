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

import pytest

from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin

# TODO: add functionality required for this test load_collection & collect
@pytest.mark.skip(reason="no way of currently testing this, since we are missing implementations for load_collection & collect")
def test_train_and_predict():
    ctx = WayangContext().register({JavaPlugin, SparkPlugin})
    
    features = ctx.load_collection([[0.0, 1.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]])
    labels   = ctx.load_collection([1.0, 1.0, 0.0, 0.0])

    model       = features.train_linear_svc(labels, max_iter=10, reg_param=0.1)
    predictions = model.predict(features)

    result = predictions.collect()
    
    print("Predictions:", result)

    assert len(result) is 4, f"Expected len(result) to be 4, but got: {len(result)}"
    for pred in result:
        assert pred in [0.0, 1.0], f"Expected prediction to be in [0.0, 0.1], but got: {pred}"
