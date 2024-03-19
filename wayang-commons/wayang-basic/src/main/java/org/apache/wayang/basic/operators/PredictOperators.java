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

package org.apache.wayang.basic.operators;

import com.fasterxml.jackson.core.type.TypeReference;

public class PredictOperators {

    public static PredictOperator<double[], Integer> kMeans() {
        // The type of TypeReference cannot be omitted, to avoid the following error.
        // error: cannot infer type arguments for TypeReference<T>, reason: cannot use '<>' with anonymous inner classes
        return new PredictOperator<>(new TypeReference<double[]>() {}, new TypeReference<Integer>() {});
    }

    public static PredictOperator<double[], Double> linearRegression() {
        return new PredictOperator<>(new TypeReference<double[]>() {}, new TypeReference<Double>() {});
    }

    public static PredictOperator<double[], Integer> decisionTreeClassification() {
        return new PredictOperator<>(new TypeReference<double[]>() {}, new TypeReference<Integer>() {});
    }
}
