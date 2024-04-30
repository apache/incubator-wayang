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
import org.apache.wayang.ml4all.abstraction.api.Compute;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

public class KMeansCompute extends Compute<Tuple2<Integer, Tuple2<Integer, double[]>>, double[]> {

    @Override
    public Tuple2 process(double[] input, ML4allModel model) {
        double[][] centers = (double[][]) model.getByKey("centers");
        double min = Double.MAX_VALUE;
        int minIndex = 0;
        for (int i = 0; i < centers.length; i++) {
            double dist = dist(input, centers[i]);
            if (dist < min) {
                min = dist;
                minIndex = i;
            }
        }

        return new Tuple2(minIndex, new Tuple2<>(1, input)); //groupby clusterID, and count
    }

    @Override
    public Tuple2<Integer, Tuple2<Integer, double[]>> aggregate(Tuple2<Integer, Tuple2<Integer, double[]>> input1, Tuple2<Integer, Tuple2<Integer, double[]>> input2) {
        Tuple2<Integer, double[]> kv1 = input1.field1;
        Tuple2<Integer, double[]> kv2 = input2.field1;
        int count = kv1.field0 + kv2.field0;
        double[] sum = new double[kv1.field1.length];
        for (int i = 0; i < kv1.field1.length; i++)
            sum[i] = kv1.field1[i] + kv2.field1[i];
        return new Tuple2(input1.field0, new Tuple2<>(count, sum));
    }

    private double dist(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++)
            sum += (a[i] - b[i])*(a[i] - b[i]);
        return Math.sqrt(sum);
    }
}
