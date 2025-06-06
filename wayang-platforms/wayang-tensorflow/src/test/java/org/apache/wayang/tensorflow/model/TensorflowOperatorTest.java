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

package org.apache.wayang.tensorflow.model;

import org.apache.wayang.basic.model.op.Mean;
import org.apache.wayang.basic.model.op.nn.*;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.tensorflow.*;
import org.tensorflow.op.Ops;
import org.tensorflow.types.TFloat32;

import java.util.Arrays;

public class TensorflowOperatorTest {

    @Test
    public void testMean() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Mean mean = new Mean(0, "mean");
            Operand<TFloat32> x = tf.array(1.0f, 2.0f, 3.0f);
            Operand<?> out = Convertor.convert(tf, mean, x);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            float ans = tensor.getFloat();
            System.out.println(ans);
            Assertions.assertEquals(2.0f, ans);
        }
    }

    @Test
    public void testSoftmax() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Softmax softmax = new Softmax("softmax");
            Operand<TFloat32> x = tf.array(0f, 0f);
            Operand<?> out = Convertor.convert(tf, softmax, x);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            float[] ans = {
                    tensor.getFloat(0),
                    tensor.getFloat(1),
            };
            System.out.println(Arrays.toString(ans));
            float[] expected = {0.5f, 0.5f};
            Assertions.assertArrayEquals(expected, ans);
        }
    }

    @Test
    public void testMSELoss() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            MSELoss mse = new MSELoss("mse");
            Operand<TFloat32> x = tf.array(1.0f, 1.0f, 1.0f);
            Operand<TFloat32> y = tf.array(2.0f, 2.0f, 2.0f);
            Operand<?> out = Convertor.convert(tf, mse, x, y);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            float ans = tensor.getFloat();
            System.out.println(ans);
            Assertions.assertEquals(1.0f, ans);
        }
    }

    @Test
    public void testConv2D() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Conv2D conv2D = new Conv2D(1, 3, new int[]{2}, new int[]{1}, "VALID", true, "conv2d");
            Operand<TFloat32> x = tf.constant(
                    new float[][][][]{{{
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f}
                    }}}
            );
            Operand<?> out = Convertor.convert(tf, conv2D, x);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            long[] shape = tensor.shape().asArray();
            System.out.println(Arrays.toString(shape));
            Assertions.assertArrayEquals(new long[]{1, 3, 2, 2}, shape);
        }
    }

    @Test
    public void testConv3D() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Conv3D conv3D = new Conv3D(1, 3, new int[]{2}, new int[]{1}, "VALID", true, "conv3d");
            float[][] one = new float[][] {
                    {1.0f, 2.0f, 3.0f},
                    {1.0f, 2.0f, 3.0f},
                    {1.0f, 2.0f, 3.0f}
            };
            Operand<TFloat32> x = tf.constant(
                    new float[][][][][]{{{
                        one, one, one
                    }}}
            );
            Operand<?> out = Convertor.convert(tf, conv3D, x);
            TFloat32 tensor = (TFloat32) session.runner().fetch("conv3d").run().get(0);
            long[] shape = tensor.shape().asArray();
            System.out.println(Arrays.toString(shape));
            Assertions.assertArrayEquals(new long[]{1, 3, 2, 2, 2}, shape);
        }
    }

    @Test
    public void testConvLSTM2D() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            ConvLSTM2D convLSTM2D = new ConvLSTM2D(1, 2, new int[]{2}, new int[]{1}, true);
            float[][] one = new float[][]{{1.0f, 2.0f, 3.0f}, {1.0f, 2.0f, 3.0f}, {1.0f, 2.0f, 3.0f}};
            Operand<TFloat32> input = tf.constant(
                    new float[][][][][]{{{one}, {one}}}
            );
            Operand<?> out = Convertor.convert(tf, convLSTM2D, input);
            out = tf.tensorMapLookup(out, tf.constant("hidden"), TFloat32.class);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            long[] shape = tensor.shape().asArray();
            System.out.println(Arrays.toString(shape));
            Assertions.assertArrayEquals(new long[]{1, 2, 3, 3}, shape);
        }
    }

    @Ignore // Ignore until the bug in TensorflowBatchNorm2D is solved.
    @Test
    public void testBatchNorm2D() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            BatchNorm2D batchNorm2D = new BatchNorm2D(1);
            Operand<TFloat32> x = tf.constant(
                    new float[][][][]{{{
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f}
                    }}}
            );
            Operand<?> out1 = Convertor.convert(g, tf, batchNorm2D, x, tf.constant(true));
            Operand<?> out2 = Convertor.convert(g, tf, batchNorm2D, x, tf.constant(false));
            Result result = session.runner().fetch(out1).fetch(out2).
                    run();
            TFloat32 tensor1 = (TFloat32) result.get(0);
            TFloat32 tensor2 = (TFloat32) result.get(1);
            float[] ans1 = new float[] {
                    tensor1.getFloat(0, 0, 0, 0),
                    tensor1.getFloat(0, 0, 0, 1),
                    tensor1.getFloat(0, 0, 0, 2)
            };
            float[] ans2 = new float[] {
                    tensor2.getFloat(0, 0, 0, 0),
                    tensor2.getFloat(0, 0, 0, 1),
                    tensor2.getFloat(0, 0, 0, 2)
            };
            System.out.println(Arrays.toString(ans1));
            System.out.println(Arrays.toString(ans2));
            Assertions.assertArrayEquals(ans1, ans2);
        }
    }
}
