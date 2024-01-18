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
import org.apache.wayang.basic.model.DecisionTreeClassificationModel;
import org.apache.wayang.basic.operators.ModelTransformOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkDecisionTreeClassificationOperator;
import org.apache.wayang.spark.operators.ml.SparkModelTransformOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SparkDecisionTreeClassificationOperatorTest extends SparkOperatorTestBase {

    public static List<Tuple2<double[], Integer>> trainingData = Arrays.asList(
            new Tuple2<>(new double[]{1, 1}, 0),
            new Tuple2<>(new double[]{2, 2}, 0),
            new Tuple2<>(new double[]{-1, -1}, 1),
            new Tuple2<>(new double[]{-2, -2}, 1),
            new Tuple2<>(new double[]{1, -1}, 2),
            new Tuple2<>(new double[]{-2, 2}, 2)
    );

    public static List<double[]> inferenceData = Arrays.asList(
            new double[]{1, 2},
            new double[]{-1, -2},
            new double[]{1, -2}
            );

    public DecisionTreeClassificationModel getModel() {
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(trainingData);
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        SparkDecisionTreeClassificationOperator decisionTreeClassificationOperator = new SparkDecisionTreeClassificationOperator();

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(decisionTreeClassificationOperator, inputs, outputs);

        // Verify the outcome.
        return output.<DecisionTreeClassificationModel>provideCollection().iterator().next();
    }

    @Test
    public void testTraining() {
        final DecisionTreeClassificationModel model = getModel();
        Assert.assertEquals(model.getDepth(), 2);
    }

    @Test
    public void testInference() {
        // Prepare test data.
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Collections.singletonList(getModel()));
        RddChannel.Instance input2 = this.createRddChannelInstance(inferenceData);
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkModelTransformOperator<double[], Integer> transformOperator = new SparkModelTransformOperator<>(ModelTransformOperator.decisionTreeClassification());

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(transformOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<double[], Integer>> results = output.<Tuple2<double[], Integer>>provideRdd().collect();
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(0, results.get(0).field1.intValue());
        Assert.assertEquals(1, results.get(1).field1.intValue());
        Assert.assertEquals(2, results.get(2).field1.intValue());
    }
}
