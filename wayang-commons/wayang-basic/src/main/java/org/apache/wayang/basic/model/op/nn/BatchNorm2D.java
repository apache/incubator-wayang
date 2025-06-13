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

package org.apache.wayang.basic.model.op.nn;

public class BatchNorm2D extends BatchNorm {

    public BatchNorm2D(int numFeatures) {
        this(numFeatures, null, DType.FLOAT32);
    }

    public BatchNorm2D(int numFeatures, String name) {
        this(numFeatures, name, DType.FLOAT32);
    }

    public BatchNorm2D(int numFeatures, DType dType) {
        this(numFeatures, null, dType);
    }

    public BatchNorm2D(int numFeatures, String name, DType dType) {
        super(numFeatures, name, dType);
    }

    public BatchNorm2D(int numFeatures, float epsilon, float momentum) {
        this(numFeatures, epsilon, momentum, null, DType.FLOAT32);
    }

    public BatchNorm2D(int numFeatures, float epsilon, float momentum, String name) {
        this(numFeatures, epsilon, momentum, name, DType.FLOAT32);
    }

    public BatchNorm2D(int numFeatures, float epsilon, float momentum, DType dType) {
        this(numFeatures, epsilon, momentum, null, dType);
    }

    public BatchNorm2D(int numFeatures, float epsilon, float momentum, String name, DType dType) {
        super(numFeatures, epsilon, momentum, name, dType);
    }
}
