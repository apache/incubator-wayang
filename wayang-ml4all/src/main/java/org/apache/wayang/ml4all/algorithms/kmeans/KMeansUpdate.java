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
import org.apache.wayang.ml4all.abstraction.api.Update;
import org.apache.wayang.ml4all.abstraction.plan.ML4allGlobalVars;

import java.util.List;

public class KMeansUpdate extends Update<Tuple2<Integer, double[]>, Tuple2<Integer, Tuple2<Integer, double[]>>> {

    @Override
    public Tuple2<Integer, double[]> process(Tuple2<Integer, Tuple2<Integer, double[]>> input, ML4allGlobalVars context) {
        int count = input.field1.field0;
        double[] newCenter = input.field1.field1;
        for (int j = 0; j < newCenter.length; j++) {
            newCenter[j] /= count;
        }
        return new Tuple2<>(input.field0, newCenter);
    }

    @Override
    public ML4allGlobalVars assign(List<Tuple2<Integer, double[]>> input, ML4allGlobalVars context) {
        double[][] centers = (double[][]) context.getByKey("centers");
        for (int i = 0; i < input.size(); i++) {
            Tuple2<Integer, double[]> c = input.get(i);
            int centroidId = c.field0;
            centers[centroidId] = c.field1;
        }
        context.put("centers", centers);
        return context;
    }
}
