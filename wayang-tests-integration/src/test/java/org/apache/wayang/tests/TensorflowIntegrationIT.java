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

package org.apache.wayang.tests;

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.*;
import org.apache.wayang.basic.model.op.nn.CrossEntropyLoss;
import org.apache.wayang.basic.model.op.nn.Linear;
import org.apache.wayang.basic.model.op.nn.Sigmoid;
import org.apache.wayang.basic.model.optimizer.GradientDescent;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.tensorflow.Tensorflow;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test the Tensorflow integration with Wayang.
 */
public class TensorflowIntegrationIT {

    public static List<float[]> trainX = Arrays.asList(
            new float[] {5.1f, 3.5f, 1.4f, 0.2f},
            new float[] {4.9f, 3.0f, 1.4f, 0.2f},
            new float[] {6.9f, 3.1f, 4.9f, 1.5f},
            new float[] {5.5f, 2.3f, 4.0f, 1.3f},
            new float[] {5.8f, 2.7f, 5.1f, 1.9f},
            new float[] {6.7f, 3.3f, 5.7f, 2.5f}
    );

    public static List<Integer> trainY = Arrays.asList(
            0, 0, 1, 1, 2, 2
    );

    public static List<float[]> testX = Arrays.asList(
            new float[] {5.0f, 3.5f, 1.6f, 0.6f},
            new float[] {5.7f, 2.8f, 4.5f, 1.3f},
            new float[] {5.6f, 2.8f, 4.9f, 2.0f}
    );

    public static List<Integer> testY = Arrays.asList(
            0, 1, 2
    );

    public static String[] LABELS = new String[]{"Iris-setosa", "Iris-versicolor", "Iris-virginica"};

    @Test
    public void test() {
        /* training features */
        CollectionSource<float[]> trainXSource = new CollectionSource<>(trainX, float[].class);

        /* training labels */
        CollectionSource<Integer> trainYSource = new CollectionSource<>(trainY, Integer.class);

        /* test features */
        CollectionSource<float[]> testXSource = new CollectionSource<>(testX, float[].class);

        /* model */
        Op l1 = new Linear(4, 64, true);
        Op s1 = new Sigmoid();
        Op l2 = new Linear(64, 3, true);
        s1.with(l1.with(new Input(Input.Type.FEATURES)));
        l2.with(s1);

        DLModel model = new DLModel(l2);

        /* training options */
        // 1. loss function
        Op criterion = new CrossEntropyLoss(3);
        criterion.with(
                new Input(Input.Type.PREDICTED, Op.DType.FLOAT32),
                new Input(Input.Type.LABEL, Op.DType.INT32)
        );

        // 2. accuracy calculation function
        Op acc = new Mean(0);
        acc.with(new Cast(Op.DType.FLOAT32).with(new Eq().with(
                new ArgMax(1).with(new Input(Input.Type.PREDICTED, Op.DType.FLOAT32)),
                new Input(Input.Type.LABEL, Op.DType.INT32)
        )));

        // 3. optimizer
        Optimizer optimizer = new GradientDescent(0.02f);

        DLTrainingOperator.Option option = new DLTrainingOperator.Option(criterion, optimizer, 6, 100);
        option.setAccuracyCalculation(acc);

        /* training operator */
        DLTrainingOperator<float[], Integer> trainingOperator = new DLTrainingOperator<>(
                model, option, float[].class, Integer.class
        );

        /* predict operator */
        PredictOperator<float[], float[]> predictOperator = new PredictOperator<>(
                float[].class, float[].class
        );

        /* map to label */
        MapOperator<float[], String> mapOperator = new MapOperator<>(array -> {
            int maxIdx = 0;
            float maxVal = array[0];
            for (int i = 1; i < array.length; i++) {
                if (array[i] > maxVal) {
                    maxIdx = i;
                    maxVal = array[i];
                }
            }
            return LABELS[maxIdx];
        }, float[].class, String.class);

        /* sink */
        List<String> predicted = new ArrayList<>();
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(predicted, String.class);

        trainXSource.connectTo(0, trainingOperator, 0);
        trainYSource.connectTo(0, trainingOperator, 1);
        trainingOperator.connectTo(0, predictOperator, 0);
        testXSource.connectTo(0, predictOperator, 1);
        predictOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, sink, 0);

        WayangPlan wayangPlan = new WayangPlan(sink);

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Tensorflow.plugin());
        wayangContext.execute(wayangPlan);

        System.out.println(predicted);
    }
}
