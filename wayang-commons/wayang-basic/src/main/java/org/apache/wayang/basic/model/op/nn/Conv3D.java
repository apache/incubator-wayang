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

public class Conv3D extends Conv {
    public Conv3D(int inChannels, int outChannels, int[] kernelSize, int[] stride, String padding, boolean bias) {
        this(inChannels, outChannels, kernelSize, stride, padding, bias, null, DType.FLOAT32);
    }

    public Conv3D(int inChannels, int outChannels, int[] kernelSize, int[] stride, String padding, boolean bias, String name) {
        this(inChannels, outChannels, kernelSize, stride, padding, bias, name, DType.FLOAT32);
    }

    public Conv3D(int inChannels, int outChannels, int[] kernelSize, int[] stride, String padding, boolean bias, DType dType) {
        this(inChannels, outChannels, kernelSize, stride, padding, bias, null, dType);
    }

    public Conv3D(int inChannels, int outChannels, int[] kernelSize, int[] stride, String padding, boolean bias, String name, DType dType) {
        super(inChannels, outChannels, kernelSize, stride, padding, bias, name, dType);
    }
}
