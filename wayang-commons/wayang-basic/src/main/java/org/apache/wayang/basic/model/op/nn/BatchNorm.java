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

public abstract class BatchNorm extends Op {
    protected final int numFeatures;
    protected final float epsilon;
    protected final float momentum;

    public BatchNorm(int numFeatures, String name, DType dType) {
        this(numFeatures, 1e-5f, 0.1f, name, dType);
    }

    public BatchNorm(int numFeatures, float epsilon, float momentum, String name, DType dType) {
        super(name, dType);
        this.numFeatures = numFeatures;
        this.epsilon = epsilon;
        this.momentum = momentum;
    }

    public int getNumFeatures() {
        return numFeatures;
    }

    public float getEpsilon() {
        return epsilon;
    }

    public float getMomentum() {
        return momentum;
    }

    @Override
    public int inputsRequired() {
        return 1;
    }
}
