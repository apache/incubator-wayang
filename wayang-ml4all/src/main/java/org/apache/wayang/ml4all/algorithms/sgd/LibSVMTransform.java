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

import org.apache.wayang.ml4all.abstraction.api.Transform;
import org.apache.wayang.ml4all.utils.StringUtil;

import java.util.List;

public class LibSVMTransform extends Transform<double[], String> {

    int features;

    public LibSVMTransform (int features) {
        this.features = features;
    }

    @Override
    public double[] transform(String line) {
        List<String> pointStr = StringUtil.split(line, ' ');
        double[] point = new double[features+1];
        point[0] = Double.parseDouble(pointStr.get(0));
        for (int i = 1; i < pointStr.size(); i++) {
            if (pointStr.get(i).equals("")) {
                continue;
            }
            String kv[] = pointStr.get(i).split(":", 2);
            point[Integer.parseInt(kv[0])] = Double.parseDouble(kv[1]);
        }
        return point;
    }
}
