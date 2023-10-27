/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spark.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkKMeansOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SparkKMeansOperatorTest extends SparkOperatorTestBase {
    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(
                new double[]{1, 2, 3},
                new double[]{-1, -2, -3},
                new double[]{2, 4, 6}));
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkKMeansOperator kMeansOperator = new SparkKMeansOperator(2);

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(kMeansOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<double[], Integer>> results = output.<Tuple2<double[], Integer>>provideRdd().collect();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(
                results.get(0).field1,
                results.get(2).field1
        );
    }
}
