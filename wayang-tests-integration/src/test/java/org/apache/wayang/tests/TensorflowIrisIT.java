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
import org.apache.wayang.basic.model.optimizer.Adam;
import org.apache.wayang.basic.model.optimizer.GradientDescent;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.tensorflow.Tensorflow;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Test the Tensorflow integration with Wayang.
 */
public class TensorflowIrisIT {

    public static URI TRAIN_PATH = createUri("/iris_train.csv");
    public static URI TEST_PATH = createUri("/iris_test.csv");

    public static Map<String, Integer> LABEL_MAP = Map.of(
            "Iris-setosa", 0,
            "Iris-versicolor", 1,
            "Iris-virginica", 2
    );

    @Test
    public void test() {
        final Tuple<Operator, Operator> trainSource = fileOperation(TRAIN_PATH, true);
        final Tuple<Operator, Operator> testSource = fileOperation(TEST_PATH, false);

        /* training features */
        Operator trainXSource = trainSource.field0;

        /* training labels */
        Operator trainYSource = trainSource.field1;

        /* test features */
        Operator testXSource = testSource.field0;

        /* test labels */
        Operator testYSource = testSource.field1;

        /* model */
        Op l1 = new Linear(4, 32, true);
        Op s1 = new Sigmoid();
        Op l2 = new Linear(32, 3, true);
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

        // 3. optimizer with learning rate
        Optimizer optimizer = new Adam(0.1f);

        // 4. batch size
        int batchSize = 45;

        // 5. epoch
        int epoch = 10;

        DLTrainingOperator.Option option = new DLTrainingOperator.Option(criterion, optimizer, batchSize, epoch);
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
        MapOperator<float[], Integer> mapOperator = new MapOperator<>(array -> {
            int maxIdx = 0;
            float maxVal = array[0];
            for (int i = 1; i < array.length; i++) {
                if (array[i] > maxVal) {
                    maxIdx = i;
                    maxVal = array[i];
                }
            }
            return maxIdx;
        }, float[].class, Integer.class);

        /* sink */
        List<Integer> predicted = new ArrayList<>();
        LocalCallbackSink<Integer> predictedSink = LocalCallbackSink.createCollectingSink(predicted, Integer.class);

        List<Integer> groundTruth = new ArrayList<>();
        LocalCallbackSink<Integer> groundTruthSink = LocalCallbackSink.createCollectingSink(groundTruth, Integer.class);

        trainXSource.connectTo(0, trainingOperator, 0);
        trainYSource.connectTo(0, trainingOperator, 1);
        trainingOperator.connectTo(0, predictOperator, 0);
        testXSource.connectTo(0, predictOperator, 1);
        predictOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, predictedSink, 0);
        testYSource.connectTo(0, groundTruthSink, 0);

        WayangPlan wayangPlan = new WayangPlan(predictedSink, groundTruthSink);

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Tensorflow.plugin());
        wayangContext.execute(wayangPlan);

        System.out.println("predicted:    " + predicted);
        System.out.println("ground truth: " + groundTruth);

        float success = 0;
        for (int i = 0; i < predicted.size(); i++) {
            if (predicted.get(i).equals(groundTruth.get(i))) {
                success += 1;
            }
        }
        System.out.println("test accuracy: " + success / predicted.size());
    }

    public static Tuple<Operator, Operator> fileOperation(URI uri, boolean random) {
        TextFileSource textFileSource = new TextFileSource(uri.toString());
        MapOperator<String, Tuple> mapOperator = new MapOperator<>(line -> {
            String[] parts = line.split(",");
            float[] x = new float[parts.length - 1];
            for (int i = 0; i < x.length; i++) {
                x[i] = Float.parseFloat(parts[i]);
            }
            int y = LABEL_MAP.get(parts[parts.length - 1]);
            return new Tuple<>(x, y);
        }, String.class, Tuple.class);

        MapOperator<Tuple, float[]> mapX = new MapOperator<>(tuple -> (float[]) tuple.field0, Tuple.class, float[].class);
        MapOperator<Tuple, Integer> mapY = new MapOperator<>(tuple -> (Integer) tuple.field1, Tuple.class, Integer.class);

        if (random) {
            Random r = new Random();
            SortOperator<String, Integer> randomOperator = new SortOperator<>(e -> r.nextInt(), String.class, Integer.class);

            textFileSource.connectTo(0, randomOperator, 0);
            randomOperator.connectTo(0, mapOperator, 0);
        } else {
            textFileSource.connectTo(0, mapOperator, 0);
        }

        mapOperator.connectTo(0, mapX, 0);
        mapOperator.connectTo(0, mapY, 0);

        return new Tuple<>(mapX, mapY);
    }

    public static URI createUri(String resourcePath) {
        try {
            return TensorflowIrisIT.class.getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }
}
