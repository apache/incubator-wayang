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

from pywy.dataquanta import WayangContext
from pywy.platforms.java import JavaPlugin
from pywy.platforms.spark import SparkPlugin
import pytest

# TODO: add functionality required for this test load_collection & collect
@pytest.mark.skip(reason="no way of currently testing this, since we are missing implementations for load_collection & collect")
def test_train_and_predict():
    # Initialize context with platforms
    ctx = WayangContext().register({JavaPlugin, SparkPlugin})

    # Input features and labels
    features = ctx.load_collection([
        [1.0, 2.0],
        [2.0, 3.0],
        [3.0, 4.0],
        [4.0, 5.0]
    ])
    labels = ctx.load_collection([3.0, 4.0, 5.0, 6.0])

    # Train the model
    model = features.train_decision_tree_regression(labels, max_depth=3, min_instances=1)

    # Run predictions on same features
    predictions = model.predict(features)

    # Collect and validate
    result = predictions.collect()
    print("Predictions:", result)

    assert len(result) is 4, f"Expected len(result) to be 4, but got: {len(result)}"
    for pred in result:
        assert pred is float
        assert pred > 1.0
        assert pred <= 7.0