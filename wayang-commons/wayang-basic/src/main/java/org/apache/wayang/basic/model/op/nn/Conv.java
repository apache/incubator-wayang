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

public abstract class Conv extends Op {
    protected final int inChannels;
    protected final int outChannels;
    protected final int[] kernelSize;
    protected final int[] stride;
    protected final String padding; // VALID, SAME
    protected final boolean bias;

    public Conv(int inChannels, int outChannels, int[] kernelSize, int[] stride, String padding, boolean bias, String name, DType dType) {
        super(name, dType);
        this.inChannels = inChannels;
        this.outChannels = outChannels;
        this.kernelSize = kernelSize;
        this.stride = stride;
        this.padding = padding;
        this.bias = bias;
    }

    public int getInChannels() {
        return inChannels;
    }

    public int getOutChannels() {
        return outChannels;
    }

    public int[] getKernelSize() {
        return kernelSize;
    }

    public int[] getStride() {
        return stride;
    }

    public String getPadding() {
        return padding;
    }

    public boolean getBias() {
        return bias;
    }

    @Override
    public int inputsRequired() {
        return 1;
    }
}
