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

import org.apache.wayang.basic.model.op.Get;
import org.apache.wayang.basic.model.op.Mean;
import org.apache.wayang.basic.model.op.Reshape;
import org.apache.wayang.basic.model.op.Slice;
import org.apache.wayang.basic.model.op.nn.*;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.tensorflow.*;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Placeholder;
import org.tensorflow.types.TBool;
import org.tensorflow.types.TFloat32;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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

    @Test
    public void testBatchNorm2D() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            BatchNorm2D batchNorm2D = new BatchNorm2D(1, 1e-5f, 0.5f);
            Operand<TFloat32> x = tf.constant(
                    new float[][][][]{{{
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f},
                            {1.0f, 2.0f, 3.0f}
                    }}}
            );
            Placeholder<TBool> trainingMode = tf.placeholder(TBool.class);
            Operand<?> out = Convertor.convert(g, tf, batchNorm2D, x, trainingMode);
            Result result1 = session.runner().feed(trainingMode, TBool.scalarOf(true))
                    .fetch("runningMean")
                    .fetch("runningVar")
                    .fetch(out)
                    .run();
            Result result2 = session.runner().feed(trainingMode, TBool.scalarOf(false))
                    .fetch("runningMean")
                    .fetch("runningVar")
                    .fetch(out)
                    .run();
//            TFloat32 tensor1 = (TFloat32) result1.get(2);
//            TFloat32 tensor2 = (TFloat32) result2.get(2);
//            float[] ans1 = new float[] {
//                    tensor1.getFloat(0, 0, 0, 0),
//                    tensor1.getFloat(0, 0, 0, 1),
//                    tensor1.getFloat(0, 0, 0, 2)
//            };
//            float[] ans2 = new float[] {
//                    tensor2.getFloat(0, 0, 0, 0),
//                    tensor2.getFloat(0, 0, 0, 1),
//                    tensor2.getFloat(0, 0, 0, 2)
//            };
            float[] ans1 = new float[] {
                    ((TFloat32) result1.get(0)).getFloat(0),
                    ((TFloat32) result1.get(1)).getFloat(0)
            };
            float[] ans2 = new float[] {
                    ((TFloat32) result2.get(0)).getFloat(0),
                    ((TFloat32) result2.get(1)).getFloat(0)
            };
            System.out.println(Arrays.toString(ans1));
            System.out.println(Arrays.toString(ans2));
            Assertions.assertArrayEquals(ans1, ans2);
        }
    }

    @Test
    public void testSlice() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            float[][] matrix = new float[][] {
                    {1.0f, 2.0f, 3.0f},
                    {4.0f, 5.0f, 6.0f},
                    {7.0f, 8.0f, 9.0f}
            };
            Operand<TFloat32> input = tf.constant(matrix);
            Slice slice = new Slice(
                    new int[][] {
                            {1,  2},
                            {2, -1},
                    }
            );
            Operand<?> out = Convertor.convert(tf, slice, input);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            long[] shape = tensor.shape().asArray();
            System.out.println(Arrays.toString(shape));
            Assertions.assertArrayEquals(new long[]{1, 1}, shape);
            float ans = tensor.getFloat(0, 0);
            System.out.println(ans);
            Assertions.assertEquals(6.0f, ans);
        }
    }

    @Test
    public void testGet() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            String key = "k";
            Get get = new Get("k");
            Operand<TFloat32> value = tf.constant(1.0f);
            Operand<?> map = tf.emptyTensorMap();
            map = tf.tensorMapInsert(map, tf.constant(key), value);
            Operand<?> out = Convertor.convert(tf, get, map);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            float ans = tensor.getFloat();
            System.out.println(ans);
            Assertions.assertEquals(1.0f, ans);
        }
    }

    @Test
    public void testReshape() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            float[][] matrix = new float[][] {
                    {1.0f, 2.0f, 3.0f},
                    {4.0f, 5.0f, 6.0f},
                    {7.0f, 8.0f, 9.0f}
            };
            Operand<TFloat32> input = tf.constant(matrix);
            Reshape reshape = new Reshape(new int[]{-1});
            Operand<?> out = Convertor.convert(tf, reshape, input);
            TFloat32 tensor = (TFloat32) session.runner().fetch(out).run().get(0);
            long[] shape = tensor.shape().asArray();
            System.out.println(Arrays.toString(shape));
            Assertions.assertArrayEquals(new long[]{9}, shape);
        }
    }

    @Test
    public void testControl() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Placeholder<TFloat32> input = tf.placeholder(TFloat32.class);
            Operand<TFloat32> x = tf.variable(tf.zeros(tf.array(1), TFloat32.class));
            Operand<TFloat32> out = tf.withControlDependencies(
                    tf.assignAdd(x, input)
            ).identity(x);

            session.runner().feed(input, TFloat32.vectorOf(1.0f)).fetch(out).fetch(x).run().get(1);
            TFloat32 tensor = (TFloat32) session.runner().feed(input, TFloat32.vectorOf(1.0f)).fetch(out).fetch(x).run().get(1);
            float ans = tensor.getFloat(0);
            System.out.println(ans);
            Assertions.assertEquals(2.0f, ans);
        }
    }

    @Test
    public void testIf() {
        try (Graph g = new Graph(); Session session = new Session(g)) {
            Ops tf = Ops.create(g);
            Placeholder<TBool> trainingMode = tf.placeholder(TBool.class);
            Operand<TFloat32> x = tf.withName("xxx").variable(tf.zeros(tf.array(1), TFloat32.class));

            // trainingMode
            Operand<TFloat32> trainOut = tf.withControlDependencies(
                tf.assignSub(x, tf.array(1.0f))
            ).identity(x);

            // inferenceMode
            Operand<TFloat32> inferOut = tf.withControlDependencies(
                    tf.assignAdd(x, tf.array(10.0f))
            ).identity(x);

            Operand<?> out = tf.ifOp(
                    trainingMode,
                    new ArrayList<>(),
                    Collections.singletonList(TFloat32.class),
                    ConcreteFunction.create(
                            Signature.builder().output("y", trainOut).build(), g
                    ),
                    ConcreteFunction.create(
                            Signature.builder().output("y", inferOut).build(), g
                    )
            ).iterator().next();

            session.runner().feed(trainingMode, TBool.scalarOf(true)).fetch(out).fetch("xxx").run();
            TFloat32 tensor = (TFloat32) session.runner().feed(trainingMode, TBool.scalarOf(false)).fetch(out).fetch("xxx").run().get(0);
            float ans = tensor.getFloat(0);
            System.out.println(ans);
            Assertions.assertEquals(9.0f, ans);
        }
    }
}
