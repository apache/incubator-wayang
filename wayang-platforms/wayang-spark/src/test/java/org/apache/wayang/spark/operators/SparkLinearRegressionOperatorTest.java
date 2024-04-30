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

import org.apache.wayang.basic.model.LinearRegressionModel;
import org.apache.wayang.basic.operators.PredictOperators;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.ml.SparkLinearRegressionOperator;
import org.apache.wayang.spark.operators.ml.SparkPredictOperator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SparkLinearRegressionOperatorTest extends SparkOperatorTestBase {

    // y = x1 + x2 + 1
    public static List<double[]> trainingX = Arrays.asList(
            new double[]{1, 1},
            new double[]{1, -1},
            new double[]{3, 2}
    );

    public static List<Double> trainingY = Arrays.asList(
            3D, 1D, 6D
    );

    public static List<double[]> inferenceData = Arrays.asList(
            new double[]{1, 2},
            new double[]{1, -2}
    );

    public LinearRegressionModel getModel() {
        // Prepare test data.
        RddChannel.Instance x = this.createRddChannelInstance(trainingX);
        RddChannel.Instance y = this.createRddChannelInstance(trainingY);
        CollectionChannel.Instance output = this.createCollectionChannelInstance();

        SparkLinearRegressionOperator linearRegressionOperator = new SparkLinearRegressionOperator(true);

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{x, y};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(linearRegressionOperator, inputs, outputs);

        // Verify the outcome.
        return output.<LinearRegressionModel>provideCollection().iterator().next();
    }

    @Test
    public void testTraining() {
        final LinearRegressionModel model = getModel();
        Assert.assertArrayEquals(new double[]{1, 1}, model.getCoefficients(), 1e-6);
        Assert.assertEquals(1, model.getIntercept(), 1e-6);
    }

    @Test
    public void testInference() {
        // Prepare test data.
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Collections.singletonList(getModel()));
        RddChannel.Instance input2 = this.createRddChannelInstance(inferenceData);
        RddChannel.Instance output = this.createRddChannelInstance();

        SparkPredictOperator<double[], Double> predictOperator = new SparkPredictOperator<>(PredictOperators.linearRegression());

        // Set up the ChannelInstances.
        ChannelInstance[] inputs = new ChannelInstance[]{input1, input2};
        ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(predictOperator, inputs, outputs);

        // Verify the outcome.
        final List<Double> results = output.<Double>provideRdd().collect();
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(4, results.get(0), 1e-6);
        Assert.assertEquals(0, results.get(1), 1e-6);
    }
}
