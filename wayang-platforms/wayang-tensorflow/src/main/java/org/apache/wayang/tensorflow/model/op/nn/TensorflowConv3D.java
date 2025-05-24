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

package org.apache.wayang.tensorflow.model.op.nn;

import org.apache.wayang.basic.model.op.nn.Conv3D;
import org.tensorflow.Operand;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Variable;
import org.tensorflow.op.nn.Conv3d;
import org.tensorflow.types.family.TNumber;

import java.util.Arrays;
import java.util.List;

public class TensorflowConv3D<T extends TNumber> {
    private final Ops tf;
    private final Conv3D op;
    private final Variable<T> kernel;
    private final Variable<T> bias;

    public TensorflowConv3D(Ops tf, Conv3D op, Class<T> tClass) {
        this.tf = tf;
        this.op = op;
        this.kernel = tf.variable(tf.random.truncatedNormal(tf.array(kernelShape()), tClass));
        if (op.getBias()) {
            bias = tf.variable(tf.random.truncatedNormal(tf.array(op.getOutChannels()), tClass));
        } else {
            bias = null;
        }
    }

    private int[] kernelShape() {
        int[] kernelSize = op.getKernelSize();
        if (kernelSize.length == 1) {
            return new int[]{kernelSize[0], kernelSize[0], kernelSize[0], op.getInChannels(), op.getOutChannels()};
        } else if (kernelSize.length == 3) {
            return new int[]{kernelSize[0], kernelSize[1], kernelSize[2], op.getInChannels(), op.getOutChannels()};
        }
        throw new RuntimeException("Unsupported Kernel: " + Arrays.toString(kernelSize));
    }

    private List<Long> strideShape() {
        int[] stride = op.getStride();
        if (stride.length == 1) {
            return Arrays.asList(1L, 1L, (long) stride[0], (long) stride[0], (long) stride[0]);
        } else if (stride.length == 3) {
            return Arrays.asList(1L, 1L, (long) stride[0], (long) stride[1], (long) stride[2]);
        }
        throw new RuntimeException("Unsupported Stride: " + Arrays.toString(stride));
    }

    public Operand<T> call(Operand<T> input) {
        if (!op.getBias()) {
            return tf.withName(op.getName()).nn.conv3d(
                    input,
                    kernel,
                    strideShape(),
                    op.getPadding(),
                    Conv3d.dataFormat("NCDHW")
            );
        } else {
            return tf.withName(op.getName()).math.add(
                    tf.nn.conv3d(
                            input,
                            kernel,
                            strideShape(),
                            op.getPadding(),
                            Conv3d.dataFormat("NCDHW")
                    ),
                    tf.reshape(bias, tf.array(-1, 1, 1, 1))
            );
        }
    }
}
