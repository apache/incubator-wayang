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

import org.apache.wayang.api.DLTrainingDataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.LoadCollectionDataQuantaBuilder;
import org.apache.wayang.api.PredictDataQuantaBuilder;
import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.*;
import org.apache.wayang.basic.model.op.nn.*;
import org.apache.wayang.basic.model.optimizer.Adam;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.basic.operators.DLTrainingOperator;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.tensorflow.Tensorflow;
import org.junit.Test;

import java.util.*;

/**
 * Test the Tensorflow ConvLSTM integration with Wayang.
 */
public class TensorflowConvLSTMIT {
    private int inputDim = 2;
    private int hiddenDim = 64;
    private int outputDim = 2;
    private int inputFrames = 6;
    private int outputFrames = 3;
    private int height = 17;
    private int width = 29;

    private int batchSize = 16;

    @Test
    public void test() {

        /* model */
        int[] kernelSize = new int[]{3, 3};
        int[] stride = new int[]{1};
        int numLayers = 3;

        Input features = new Input(new int[]{-1, inputFrames, inputDim, height, width}, Input.Type.FEATURES);
        Input labels = new Input(new int[]{-1, outputFrames, outputDim, height, width}, Input.Type.LABEL);

        int[] perm = new int[]{0, 2, 1, 3, 4};

        DLModel.Builder builder = new DLModel.Builder();
        builder.layer(features)
                .layer(new ConvLSTM2D(inputDim, hiddenDim, kernelSize, stride, true, "output", "convlstm1"))
//                .layer(new Transpose(perm)) // change channels and timeStep
//                .layer(new BatchNorm3D(hiddenDim, "batchnorm1")) FIXME: bug in BatchNorm3D, currently not use
//                .layer(new Transpose(perm)) // change channels and timeStep
        ;
        for (int i = 2; i <= numLayers; i++) {
            builder.layer(new ConvLSTM2D(hiddenDim, hiddenDim, kernelSize, stride, true, "output", "convlstm" + i))
//                    .layer(new Transpose(perm))
//                    .layer(new BatchNorm3D(hiddenDim, "batchnorm" + i))
//                    .layer(new Transpose(perm))
            ;
        }
        builder.layer(new Slice(new int[][]{{0, -1}, {inputFrames - outputFrames, -1}, {0, -1}, {0, -1}, {0, -1}})) // Input only the last outputFrames from ConvLSTM
                .layer(new Reshape(new int[]{-1, hiddenDim * outputFrames, height, width}))
                .layer(new Conv2D(hiddenDim * outputFrames, outputDim * outputFrames, kernelSize, stride, "SAME", true))
//                .layer(new Transpose(perm)) // change channels and timeStep
//                .layer(new Conv3D(hiddenDim, outputDim, new int[]{3, 3, 3}, stride, "SAME", true)) // FIXME: The gradient of conv3D cannot be calculated, use conv2D as a substitute.
//                .layer(new Transpose(perm)) // change channels and timeStep
        ;
        DLModel model = builder.build();

        /* training options */
        // 1. loss function
        Op criterion = new MSELoss();
        criterion.with(
                new Reshape(new int[]{-1}).with(model.getOut()),
                new Reshape(new int[]{-1}).with(labels)
//                new Reshape(new int[]{-1}).with(new ZeroLike().with(model.getOut()))
        );

        // 2. optimizer
        float learningRate = 0.1f;
        Optimizer optimizer = new Adam(learningRate);

        int epoch = 10;
        DLTrainingOperator.Option option = new DLTrainingOperator.Option(criterion, optimizer, batchSize, epoch);

        WayangContext wayangContext = new WayangContext()
                .with(Java.basicPlugin())
                .with(Tensorflow.plugin());

        JavaPlanBuilder plan = new JavaPlanBuilder(wayangContext);

        LoadCollectionDataQuantaBuilder<float[][][][]> X = plan.loadCollection(mockData(inputFrames, inputDim));
        LoadCollectionDataQuantaBuilder<float[][][][]> XTest = plan.loadCollection(mockData(inputFrames, inputDim));
        LoadCollectionDataQuantaBuilder<float[][][][]> Y = plan.loadCollection(mockData(outputFrames, outputDim));

        DLTrainingDataQuantaBuilder<float[][][][], float[][][][]> trainingOperator = X.dlTraining(Y, model, option);

//        Collection<DLModel> trainedModel = trainingOperator.collect();
//        System.out.println(trainedModel);

        PredictDataQuantaBuilder<float[][][][], float[][][][]> predictOperator = trainingOperator.predict(XTest, float[][][][].class);
        Collection<float[][][][]> predicted = predictOperator.collect();
        System.out.println(Arrays.deepToString(predicted.iterator().next()));
    }

    /**
     *
     * @return [count, timeStep, inputDim or outputDim, height, width]
     */
    public List<float[][][][]> mockData(int timeStep, int dim) {
        int count = batchSize * 2;

        List<float[][][][]> data = new ArrayList<>();
        Random rand = new Random();
        for (int c = 0; c < count; c++) {
            float[][][][] tensor = new float[timeStep][dim][height][width];
            for (int t = 0; t < timeStep; t++) {
                for (int d = 0; d < dim; d++) {
                    for (int h = 0; h < height; h++) {
                        for (int w = 0; w < width; w++) {
                            tensor[t][d][h][w] = rand.nextFloat();
                        }
                    }
                }
            }
            data.add(tensor);
        }
        return data;
    }
}
