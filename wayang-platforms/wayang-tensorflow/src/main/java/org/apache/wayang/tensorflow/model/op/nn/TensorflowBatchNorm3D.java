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

import org.apache.wayang.basic.model.op.nn.BatchNorm2D;
import org.apache.wayang.basic.model.op.nn.BatchNorm3D;
import org.tensorflow.Graph;
import org.tensorflow.Operand;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Shape;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.family.TNumber;

import java.util.Arrays;

public class TensorflowBatchNorm3D<T extends TNumber> {
    private final Ops tf;
    private final BatchNorm3D op;
    private final TensorflowBatchNorm2D<T> batchNorm2D;

    public TensorflowBatchNorm3D(Graph graph, Ops tf, BatchNorm3D op, Class<T> tClass) {
        this.tf = tf;
        this.op = op;
        BatchNorm2D op2 = new BatchNorm2D(
                op.getNumFeatures(),
                op.getEpsilon(),
                op.getMomentum(),
                op.getDType()
        );
        batchNorm2D = new TensorflowBatchNorm2D<>(graph, tf, op2, tClass);
    }

    public Operand<T> call(Operand<T> input, Operand<TBool> trainingMode) {
        // input: N, C, D, H, W
        Shape<TInt32> inputShape = tf.shape(input);
        Operand<TInt32> square = tf.math.mul(tf.shape.size(inputShape, tf.constant(3)), tf.shape.size(inputShape, tf.constant(4)));
        Operand<TInt32> newShape = tf.concat(
                Arrays.asList(
                        tf.shape.take(inputShape, tf.constant(3)),
                        square
                ),
                tf.constant(0)
        ); // N, C, D, H * W
        Operand<T> input2D = tf.reshape(input, newShape);
        Operand<T> output = batchNorm2D.call(input2D, trainingMode);
        return tf.withName(op.getName()).reshape(output, inputShape);
    }
}
