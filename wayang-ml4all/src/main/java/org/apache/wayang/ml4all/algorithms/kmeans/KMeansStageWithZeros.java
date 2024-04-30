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

import org.apache.wayang.ml4all.abstraction.api.LocalStage;
import org.apache.wayang.ml4all.abstraction.plan.ML4allModel;

public class KMeansStageWithZeros extends LocalStage {

    int k, dimension;

    public KMeansStageWithZeros(int k, int dimension) {
        this.k = k;
        this.dimension = dimension;
    }

    @Override
    public void staging (ML4allModel model) {
        double[][] centers = new double[k][];
        for (int i = 0; i < k; i++) {
            centers[i] = new double[dimension];
        }
        model.put("centers", centers);
    }
}
