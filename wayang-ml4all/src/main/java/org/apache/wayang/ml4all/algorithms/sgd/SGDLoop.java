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

package org.apache.wayang.ml4all.algorithms.sgd;

import org.apache.wayang.ml4all.abstraction.api.Loop;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

public class SGDLoop extends Loop<Double, double[]> {

    public double accuracy;
    public int maxIterations;
    int currentIteration;

    public SGDLoop(double accuracy, int maxIterations) {
        this.accuracy = accuracy;
        this.maxIterations = maxIterations;
    }

    @Override
    public Double prepareConvergenceDataset(double[] input, ML4allModel model) {
        double[] weights = (double[]) model.getByKey("weights");
        double delta = 0.0;
        for (int j = 0; j < weights.length; j++) {
            delta += Math.abs(weights[j] - input[j]);
        }
        return delta;
    }

    @Override
    public boolean terminate(Double input) {
        return (input < accuracy || ++currentIteration >= maxIterations);
    }

}
