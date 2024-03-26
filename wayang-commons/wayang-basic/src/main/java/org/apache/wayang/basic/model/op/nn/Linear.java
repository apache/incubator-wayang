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

import org.apache.wayang.basic.model.op.Op;

public class Linear extends Op {
    private final int inFeatures;
    private final int outFeatures;
    private final boolean bias;

    public Linear(int inFeatures, int outFeatures, boolean bias) {
        this(inFeatures, outFeatures, bias, DType.FLOAT32);
    }

    public Linear(int inFeatures, int outFeatures, boolean bias, String name) {
        this(inFeatures, outFeatures, bias, name, DType.FLOAT32);
    }

    public Linear(int inFeatures, int outFeatures, boolean bias, DType dType) {
        super(dType);
        this.inFeatures = inFeatures;
        this.outFeatures = outFeatures;
        this.bias = bias;
    }

    public Linear(int inFeatures, int outFeatures, boolean bias, String name, DType dType) {
        super(name, dType);
        this.inFeatures = inFeatures;
        this.outFeatures = outFeatures;
        this.bias = bias;
    }

    public int getInFeatures() {
        return inFeatures;
    }

    public int getOutFeatures() {
        return outFeatures;
    }

    public boolean getBias() {
        return bias;
    }

    @Override
    public int inputsRequired() {
        return 1;
    }
}
