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

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.*;
import org.apache.wayang.basic.model.op.nn.CrossEntropyLoss;
import org.apache.wayang.basic.model.op.nn.Linear;
import org.apache.wayang.basic.model.op.nn.Sigmoid;
import org.apache.wayang.basic.model.optimizer.GradientDescent;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.junit.Test;
import org.tensorflow.ndarray.FloatNdArray;
import org.tensorflow.ndarray.IntNdArray;
import org.tensorflow.ndarray.NdArrays;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.op.Ops;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TInt32;

public class TensorflowModelTest {

    @Test
    public void test() {
        FloatNdArray x = NdArrays.ofFloats(Shape.of(6, 4))
                .set(NdArrays.vectorOf(5.1f, 3.5f, 1.4f, 0.2f), 0)
                .set(NdArrays.vectorOf(4.9f, 3.0f, 1.4f, 0.2f), 1)
                .set(NdArrays.vectorOf(6.9f, 3.1f, 4.9f, 1.5f), 2)
                .set(NdArrays.vectorOf(5.5f, 2.3f, 4.0f, 1.3f), 3)
                .set(NdArrays.vectorOf(5.8f, 2.7f, 5.1f, 1.9f), 4)
                .set(NdArrays.vectorOf(6.7f, 3.3f, 5.7f, 2.5f), 5)
        ;

        IntNdArray y = NdArrays.vectorOf(0, 0, 1, 1, 2, 2);


        Op l1 = new Linear(4, 64, true);
        Op s1 = new Sigmoid();
        Op l2 = new Linear(64, 3, true);
        s1.with(l1.with(new Input(Input.Type.FEATURES)));
        l2.with(s1);

        DLModel model = new DLModel(l2);

        Op criterion = new CrossEntropyLoss(3);
        criterion.with(
                new Input(Input.Type.PREDICTED, Op.DType.FLOAT32),
                new Input(Input.Type.LABEL, Op.DType.INT32)
        );

        Op acc = new Mean(0);
        acc.with(new Cast(Op.DType.FLOAT32).with(new Eq().with(
                new ArgMax(1).with(new Input(Input.Type.PREDICTED, Op.DType.FLOAT32)),
                new Input(Input.Type.LABEL, Op.DType.INT32)
        )));

        Optimizer optimizer = new GradientDescent(0.02f);

        try (TensorflowModel tfModel = new TensorflowModel(model, criterion, optimizer, acc)) {
            System.out.println(tfModel.getOut().getName());

            tfModel.train(x, y, 100, 6);
            TFloat32 predicted = tfModel.predict(x);
            Ops tf = Ops.create();
            org.tensorflow.op.math.ArgMax<TInt32> argMax = tf.math.argMax(tf.constantOf(predicted), tf.constant(1), TInt32.class);
            final TInt32 tensor = argMax.asTensor();

            System.out.print("[ ");
            for (int i = 0; i < tensor.shape().size(0); i++) {
                System.out.print(tensor.getInt(i) + " ");
            }
            System.out.println("]");
        }

        System.out.println();
    }
}