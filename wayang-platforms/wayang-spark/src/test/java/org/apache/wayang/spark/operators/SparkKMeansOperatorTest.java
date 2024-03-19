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

import org.apache.wayang.basic.model.KMeansModel;
import org.apache.wayang.basic.operators.PredictOperators;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkKMeansOperator;
import org.apache.wayang.spark.operators.ml.SparkPredictOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SparkKMeansOperatorTest extends SparkOperatorTestBase {

    public static List<double[]> data = Arrays.asList(
            new double[]{1, 2, 3},
            new double[]{-1, -2, -3},
            new double[]{2, 4, 6}
    );

    public KMeansModel getModel() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(data);
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        SparkKMeansOperator kMeansOperator = new SparkKMeansOperator(2);

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(kMeansOperator, inputs, outputs);

        // Verify the outcome.
        return output.<KMeansModel>provideCollection().iterator().next();
    }

    @Test
    public void testTraining() {
        final KMeansModel model = getModel();
        Assert.assertEquals(2, model.getK());
        List<double[]> centers = Arrays.stream(model.getClusterCenters())
                .sorted(Comparator.comparingDouble(a -> a[0]))
                .collect(Collectors.toList());
        Assert.assertArrayEquals(centers.get(0), new double[]{-1.0, -2.0, -3.0}, 0.1);
        Assert.assertArrayEquals(centers.get(1), new double[]{1.5, 3.0, 4.5}, 0.1);
    }

    @Test
    public void testInference() {
        // Prepare test data.
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Collections.singletonList(getModel()));
        RddChannel.Instance input2 = this.createRddChannelInstance(data);
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkPredictOperator<double[], Integer> predictOperator = new SparkPredictOperator<>(PredictOperators.kMeans());

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(predictOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> results = output.<Integer>provideRdd().collect();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(
                results.get(0),
                results.get(2)
        );
    }
}
