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

import org.apache.wayang.basic.model.op.nn.Linear;
import org.tensorflow.Operand;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Variable;
import org.tensorflow.types.family.TNumber;

public class TensorflowLinear<T extends TNumber> {
    private final Ops tf;
    private final Linear op;
    private final Variable<T> weights;
    private final Variable<T> bias;

    public TensorflowLinear(Ops tf, Linear op, Class<T> tClass) {
        this.tf = tf;
        this.op = op;
        this.weights = tf.withName("LinearWeights").variable(tf.random.truncatedNormal(tf.array(op.getInFeatures(), op.getOutFeatures()), tClass));
        if (op.getBias()) {
            bias = tf.withName("LinearBias").variable(tf.random.truncatedNormal(tf.array(op.getOutFeatures()), tClass));
        } else {
            bias = null;
        }
    }

    public Operand<T> call(Operand<T> input) {
        // input: [batch_size, input_dim]
        if (!op.getBias()) {
            return tf.withName(op.getName()).linalg.matMul(
                    input,
                    weights // [input_dim, output_dim]
            );
        } else {
            return tf.withName(op.getName()).math.add(
                    tf.linalg.matMul(
                            input,
                            weights
                    ),
                    bias
            );
        }
    }
}
