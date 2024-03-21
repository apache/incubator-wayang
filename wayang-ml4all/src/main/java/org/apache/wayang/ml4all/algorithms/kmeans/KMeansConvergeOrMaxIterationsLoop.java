/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.algorithms.kmeans;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.ml4all.abstraction.api.Loop;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

import java.util.ArrayList;

public class KMeansConvergeOrMaxIterationsLoop extends Loop<Double, ArrayList<Tuple2<Integer, double[]>>> {

    private double accuracy;
    private int maxIterations;

    private int currentIteration = 0;

    public KMeansConvergeOrMaxIterationsLoop(double accuracy, int maxIterations) {
        this.accuracy = accuracy;
        this.maxIterations = maxIterations;
    }

    @Override
    public Double prepareConvergenceDataset(ArrayList<Tuple2<Integer, double[]>> newCenters, ML4allModel model) {
        double[][] centers = (double[][]) model.getByKey("centers");
        double delta = 0.0;
        int dimension = centers[0].length;
        for (int i = 0; i < newCenters.size(); i++) {
            int centroidId = newCenters.get(i).field0;
            for (int j = 0; j < dimension; j++) {
                delta += Math.abs(centers[centroidId][j] - newCenters.get(i).field1[j]);
            }
        }
        return delta;
    }

    @Override
    public boolean terminate(Double input) {
        return input < accuracy || ++currentIteration >= maxIterations;
    }

}
