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

import org.apache.wayang.api.*;
import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.*;
import org.apache.wayang.basic.model.op.nn.CrossEntropyLoss;
import org.apache.wayang.basic.model.op.nn.Linear;
import org.apache.wayang.basic.model.op.nn.Sigmoid;
import org.apache.wayang.basic.model.optimizer.Adam;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.basic.operators.DLTrainingOperator;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.tensorflow.Tensorflow;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Test the Tensorflow integration with Wayang.
 * Note: this test fails on M1 Macs because of Tensorflow-Java incompatibility.
 */
public class TensorflowIrisScalaLikeApiIT {

    public static URI TRAIN_PATH = createUri("/iris_train.csv");
    public static URI TEST_PATH = createUri("/iris_test.csv");

    public static Map<String, Integer> LABEL_MAP = Map.of(
            "Iris-setosa", 0,
            "Iris-versicolor", 1,
            "Iris-virginica", 2
    );

    @Test
    void test() {
        WayangContext wayangContext = new WayangContext()
                .with(Java.basicPlugin())
                .with(Tensorflow.plugin());

        JavaPlanBuilder plan = new JavaPlanBuilder(wayangContext);

        final Tuple<DataQuantaBuilder<?, float[]>, DataQuantaBuilder<?, Integer>> trainSource =
                fileOperation(plan, TRAIN_PATH, true);
        final Tuple<DataQuantaBuilder<?, float[]>, DataQuantaBuilder<?, Integer>> testSource =
                fileOperation(plan, TEST_PATH, false);

        /* training features */
        DataQuantaBuilder<?, float[]> trainXSource = trainSource.field0;

        /* training labels */
        DataQuantaBuilder<?, Integer> trainYSource = trainSource.field1;

        /* test features */
        DataQuantaBuilder<?, float[]> testXSource = testSource.field0;

        /* test labels */
        DataQuantaBuilder<?, Integer> testYSource = testSource.field1;

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
        DLTrainingDataQuantaBuilder<float[], Integer> trainingOperator =
                trainXSource.dlTraining(trainYSource, model, option);

        /* predict operator */
        PredictDataQuantaBuilder<float[], float[]> predictOperator =
                trainingOperator.predict(testXSource, float[].class);

        /* map to label */
        MapDataQuantaBuilder<float[], Integer> mapOperator = predictOperator.map(array -> {
            int maxIdx = 0;
            float maxVal = array[0];
            for (int i = 1; i < array.length; i++) {
                if (array[i] > maxVal) {
                    maxIdx = i;
                    maxVal = array[i];
                }
            }
            return maxIdx;
        });

        /* sink */
        List<Integer> predicted = new ArrayList<>(mapOperator.collect());
        // fixme: Currently, wayang's scala-like api only supports a single collect,
        //  so it is not possible to collect multiple result lists in a single plan.
//        List<Integer> groundTruth = new ArrayList<>(testYSource.collect());

        System.out.println("predicted:    " + predicted);
//        System.out.println("ground truth: " + groundTruth);

//        float success = 0;
//        for (int i = 0; i < predicted.size(); i++) {
//            if (predicted.get(i).equals(groundTruth.get(i))) {
//                success += 1;
//            }
//        }
//        System.out.println("test accuracy: " + success / predicted.size());
    }

    public static Tuple<DataQuantaBuilder<?, float[]>, DataQuantaBuilder<?, Integer>>
    fileOperation(JavaPlanBuilder plan, URI uri, boolean random) {
        DataQuantaBuilder<?, String> textFileSource = plan.readTextFile(uri.toString());

        if (random) {
            Random r = new Random();
            textFileSource = textFileSource.sort(e -> r.nextInt());
        }

        MapDataQuantaBuilder<String, Tuple<float[], Integer>> mapXY = textFileSource.map(line -> {
                    String[] parts = line.split(",");
                    float[] x = new float[parts.length - 1];
                    for (int i = 0; i < x.length; i++) {
                        x[i] = Float.parseFloat(parts[i]);
                    }
                    int y = LABEL_MAP.get(parts[parts.length - 1]);
                    return new Tuple<>(x, y);
                });

        MapDataQuantaBuilder<Tuple<float[], Integer>, float[]> mapX = mapXY.map(tuple -> tuple.field0);
        MapDataQuantaBuilder<Tuple<float[], Integer>, Integer> mapY = mapXY.map(tuple -> tuple.field1);

        return new Tuple<>(mapX, mapY);
    }

    public static URI createUri(String resourcePath) {
        try {
            return TensorflowIrisScalaLikeApiIT.class.getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }
    }
}
