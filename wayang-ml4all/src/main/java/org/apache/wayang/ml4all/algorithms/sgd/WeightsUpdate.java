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

import org.apache.wayang.ml4all.abstraction.api.UpdateLocal;
import org.apache.wayang.ml4all.abstraction.plan.ML4allContext;

public class WeightsUpdate extends UpdateLocal<double[], double[]> {

    double[] weights;
    int current_iteration;

    double stepSize = 1;
    double regulizer = 0;

    public WeightsUpdate () { }

    public WeightsUpdate (double stepSize, double regulizer) {
        this.stepSize = stepSize;
        this.regulizer = regulizer;
    }

    @Override
    public double[] process(double[] input, ML4allContext context) {
        double[] weights = (double[]) context.getByKey("weights");
        double count = input[0];
        int current_iteration = (int) context.getByKey("iter");
        double alpha = stepSize / current_iteration;
        double[] newWeights = new double[weights.length];
        for (int j = 0; j < weights.length; j++) {
            newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * input[j + 1];
        }
        return newWeights;
    }

    @Override
    public ML4allContext assign(double[] input, ML4allContext context) {
        context.put("weights", input);
        int iteration = (int) context.getByKey("iter");
        context.put("iter", ++iteration);
        return context;
    }


}
