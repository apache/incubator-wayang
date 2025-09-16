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

import org.apache.wayang.basic.model.op.nn.Conv2D;
import org.apache.wayang.basic.model.op.nn.ConvLSTM2D;
import org.tensorflow.Operand;
import org.tensorflow.Output;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Stack;
import org.tensorflow.types.TInt32;
import org.tensorflow.types.family.TNumber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TensorflowConvLSTM2D<T extends TNumber> {
    private final Ops tf;
    private final ConvLSTM2D op;
    private final Cell<T> cell;
    private final Class<T> tClass;

    public TensorflowConvLSTM2D(Ops tf, ConvLSTM2D op, Class<T> tClass) {
        this.tf = tf;
        this.op = op;
        this.tClass = tClass;
        this.cell = new Cell<T>(tf, op, tClass);
    }

    public Operand<?> call(Operand<T> input) {
        // input: [batch_size, time_step, input_dim, height, width]
        Operand<TInt32> shape = tf.concat(
                Arrays.asList(
                        tf.shape.size(tf.shape(input), tf.constant(0)), // batch_size
                        tf.array(op.getHiddenDim()), // hidden_dim
                        tf.shape.size(tf.shape(input), tf.constant(3)), // height
                        tf.shape.size(tf.shape(input), tf.constant(4)) // width
                ),
                tf.constant(0)
        );

        Operand<T> h = tf.zeros(shape, tClass);
        Operand<T> c = tf.zeros(shape, tClass);

        String outKey = op.getOutput();
        long seqLen = input.shape().get(1);
        List<Operand<T>> outputs = new ArrayList<>((int) seqLen);

        for (long t = 0; t < seqLen; t++) {
            Operand<T>[] hc = cell.call(tf.gather(input, tf.constant(t), tf.constant(1)), h, c);
            h = hc[0];
            c = hc[1];
            if ("output".equals(outKey)) {
                outputs.add(h);
            }
        }

        if ("output".equals(outKey)) {
            return tf.stack(outputs, Stack.axis(1L));
        }
        if ("hidden".equals(outKey)) {
            return h;
        }
        if ("cell".equals(outKey)) {
            return c;
        }

        throw new IllegalArgumentException("Unrecognized output: " + outKey);
    }

    public static class Cell<T extends TNumber> {
        private final Ops tf;
        private final TensorflowConv2D<T> conv;

        public Cell(Ops tf, ConvLSTM2D op, Class<T> tClass) {
            this.tf = tf;
            this.conv = new TensorflowConv2D<>(tf, new Conv2D(
                    op.getInputDim() + op.getHiddenDim(), op.getHiddenDim() * 4,
                    op.getKernelSize(), op.getStride(), "SAME", op.getBias(), op.getDType()
            ), tClass);
        }

        public Operand<T>[] call(Operand<T> input, Operand<T> hCur, Operand<T> cCur) {
            // input: [batch_size, input_dim, height, width]
            // hCur: [batch_size, hidden_dim, height, width]
            // cCur: [batch_size, hidden_dim, height, width]
            Operand<T> combined = tf.concat(Arrays.asList(input, hCur), tf.constant(1)); // [batch_size, input_dim + hidden_dim, height, width]
            Operand<T> combinedConv = conv.call(combined); // [batch_size, 4 * hidden_dim, height, width]
            List<Output<T>> split = tf.split(tf.constant(1), combinedConv, 4L).output();
            Operand<T> i = tf.math.sigmoid(split.get(0)); // [batch_size, hidden_dim, height, width]
            Operand<T> f = tf.math.sigmoid(split.get(1)); // [batch_size, hidden_dim, height, width]
            Operand<T> o = tf.math.sigmoid(split.get(2)); // [batch_size, hidden_dim, height, width]
            Operand<T> g = tf.math.tanh(split.get(3)); // [batch_size, hidden_dim, height, width]

            Operand<T> cNext = tf.math.add(tf.math.mul(f, cCur), tf.math.mul(i, g)); // [batch_size, hidden_dim, height, width]
            Operand<T> hNext = tf.math.mul(o, tf.math.tanh(cNext)); // [batch_size, hidden_dim, height, width]

            return new Operand[]{hNext, cNext};
        }
    }
}
