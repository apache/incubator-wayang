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

import org.apache.wayang.basic.model.op.*;
import org.apache.wayang.basic.model.op.nn.*;
import org.apache.wayang.basic.model.optimizer.Adam;
import org.apache.wayang.basic.model.optimizer.GradientDescent;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.tensorflow.Graph;
import org.tensorflow.Operand;
import org.tensorflow.Tensor;
import org.tensorflow.ndarray.*;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Variable;
import org.tensorflow.types.*;
import org.tensorflow.types.family.TNumber;
import org.tensorflow.types.family.TType;

public class Convertor {

    public static Operand<?> convert(Ops tf, Op op, Operand<?>... inputs) {
        if (op instanceof ArgMax) {
            return convert(tf, (ArgMax) op, inputs[0]);
        }
        if (op instanceof Cast) {
            return convert(tf, (Cast) op, inputs[0]);
        }
        if (op instanceof CrossEntropyLoss) {
            return convert(tf, (CrossEntropyLoss) op, inputs[0], inputs[1]);
        }
        if (op instanceof Eq) {
            return convert(tf, (Eq) op, inputs[0], inputs[1]);
        }
        if (op instanceof Input) {
            return convert(tf, (Input) op);
        }
        if (op instanceof Linear) {
            return convert(tf, (Linear) op, inputs[0]);
        }
        if (op instanceof Mean) {
            return convert(tf, (Mean) op, inputs[0]);
        }
        if (op instanceof ReLU) {
            return convert(tf, (ReLU) op, inputs[0]);
        }
        if (op instanceof Sigmoid) {
            return convert(tf, (Sigmoid) op, inputs[0]);
        }
        if (op instanceof Softmax) {
            return convert(tf, (Softmax) op, inputs[0]);
        }


        throw new RuntimeException("Unsupported operator: " + op.getClass());
    }

    public static Operand<TInt32> convert(Ops tf, ArgMax op, Operand<?> input) {
        return tf.withName(op.getName()).math.argMax(input, tf.constant(op.getDim()), TInt32.class);
    }

    public static Operand<?> convert(Ops tf, Cast op, Operand<?> input) {
        if (op.getDType() == Op.DType.INT32) {
            return tf.withName(op.getName()).dtypes.cast(input, TInt32.class);
        }
        if (op.getDType() == Op.DType.INT64) {
            return tf.withName(op.getName()).dtypes.cast(input, TInt64.class);
        }
        if (op.getDType() == Op.DType.FLOAT32) {
            return tf.withName(op.getName()).dtypes.cast(input, TFloat32.class);
        }
        if (op.getDType() == Op.DType.FLOAT64) {
            return tf.withName(op.getName()).dtypes.cast(input, TFloat64.class);
        }
        if (op.getDType() == Op.DType.BYTE) {
            return tf.withName(op.getName()).dtypes.cast(input, TUint8.class);
        }
        if (op.getDType() == Op.DType.BOOL) {
            return tf.withName(op.getName()).dtypes.cast(input, TBool.class);
        }

        throw new RuntimeException("Unsupported DType: " + op.getDType());
    }

    public static Operand<?> convert(Ops tf, CrossEntropyLoss op, Operand<?> predicted, Operand<?> labels) {
        Operand<?> oneHot;
        if (op.getDType() == Op.DType.FLOAT32) {
            oneHot = tf.oneHot((Operand<? extends TNumber>) labels, tf.constant(op.getLabels()), tf.constant(1.0f), tf.constant(0.0f));
        } else if (op.getDType() == Op.DType.FLOAT64) {
            oneHot = tf.oneHot((Operand<? extends TNumber>) labels, tf.constant(op.getLabels()), tf.constant(1.0), tf.constant(0.0));
        } else {
            throw new RuntimeException("Unsupported DType: " + op.getDType());
        }

        return tf.withName(op.getName()).math.mean(
                tf.math.neg(tf.reduceSum(
                        tf.math.mul(
                                tf.math.log(tf.nn.softmax((Operand<TNumber>) predicted)),
                                (Operand<TNumber>) oneHot
                        ),
                        tf.array(1)
                )),
                tf.array(0)
        );
    }

    public static Operand<TBool> convert(Ops tf, Eq op, Operand<?> left, Operand<?> right) {
        return tf.withName(op.getName()).math.equal(
                (Operand<TType>) left,
                (Operand<TType>) right
        );
    }

    public static Operand<?> convert(Ops tf, Input op) {
        if (op.getDType() == Op.DType.INT32) {
            return tf.withName(op.getName()).placeholder(TInt32.class);
        }
        if (op.getDType() == Op.DType.INT64) {
            return tf.withName(op.getName()).placeholder(TInt64.class);
        }
        if (op.getDType() == Op.DType.FLOAT32) {
            return tf.withName(op.getName()).placeholder(TFloat32.class);
        }
        if (op.getDType() == Op.DType.FLOAT64) {
            return tf.withName(op.getName()).placeholder(TFloat64.class);
        }
        if (op.getDType() == Op.DType.BYTE) {
            return tf.withName(op.getName()).placeholder(TUint8.class);
        }
        if (op.getDType() == Op.DType.BOOL) {
            return tf.withName(op.getName()).placeholder(TBool.class);
        }

        throw new RuntimeException("Unsupported DType: " + op.getDType());
    }

    public static Operand<?> convert(Ops tf, Linear op, Operand<?> input) {
        if (op.getDType() == Op.DType.INT32) {
            return convertInternal(tf, op, (Operand<TInt32>) input, TInt32.class);
        }
        if (op.getDType() == Op.DType.INT64) {
            return convertInternal(tf, op, (Operand<TInt64>) input, TInt64.class);
        }
        if (op.getDType() == Op.DType.FLOAT32) {
            return convertInternal(tf, op, (Operand<TFloat32>) input, TFloat32.class);
        }
        if (op.getDType() == Op.DType.FLOAT64) {
            return convertInternal(tf, op, (Operand<TFloat64>) input, TFloat64.class);
        }
        if (op.getDType() == Op.DType.BYTE) {
            return convertInternal(tf, op, (Operand<TUint8>) input, TUint8.class);
        }

        throw new RuntimeException("Unsupported DType: " + op.getDType());
    }

    public static <T extends TNumber> Operand<T> convertInternal(Ops tf, Linear op, Operand<T> input, Class<T> tClass) {
        int[] weightShape = new int[] {op.getInFeatures(), op.getOutFeatures()};
        Variable<T> weights = tf.variable(tf.random.truncatedNormal(tf.array(weightShape), tClass));
        if (!op.getBias()) {
            return tf.withName(op.getName()).linalg.matMul(
                    input,
                    weights
            );
        }
        int biasShape = op.getOutFeatures();
        Variable<T> biases = tf.variable(tf.random.truncatedNormal(tf.array(biasShape), tClass));
        return tf.withName(op.getName()).math.add(
                tf.linalg.matMul(
                        input,
                        weights
                ),
                biases
        );
    }

    public static Operand<?> convert(Ops tf, Mean op, Operand<?> input) {
        return tf.withName(op.getName()).math.mean(input, tf.constant(op.getDim()));
    }

    public static Operand<?> convert(Ops tf, ReLU op, Operand<?> input) {
        return tf.withName(op.getName()).nn.relu((Operand<? extends TNumber>) input);
    }

    public static Operand<?> convert(Ops tf, Sigmoid op, Operand<?> input) {
        return tf.withName(op.getName()).math.sigmoid(input);
    }

    public static Operand<?> convert(Ops tf, Softmax op, Operand<?> input) {
        return tf.withName(op.getName()).nn.softmax((Operand<? extends TNumber>) input);
    }

    public static org.tensorflow.framework.optimizers.Optimizer convert(Graph graph, Optimizer optimizer) {
        if (optimizer instanceof GradientDescent) {
            GradientDescent gd = (GradientDescent) optimizer;
            return new org.tensorflow.framework.optimizers.GradientDescent(graph, gd.getName(), gd.getLearningRate());
        }
        if (optimizer instanceof Adam) {
            Adam adam = (Adam) optimizer;
            return new org.tensorflow.framework.optimizers.Adam(graph, adam.getName(), adam.getLearningRate(), adam.getBetaOne(), adam.getBetaTwo(), adam.getEpsilon());
        }

        throw new RuntimeException("Unsupported optimizer: " + optimizer.getClass());
    }

    public static Tensor ndArrayToTensor(NdArray<?> array) {
        if (array instanceof Tensor) {
            return (Tensor) array;
        }

        if (array instanceof IntNdArray) {
            return TInt32.tensorOf((IntNdArray) array);
        }
        else if (array instanceof LongNdArray) {
            return TInt64.tensorOf((LongNdArray) array);
        }
        else if (array instanceof FloatNdArray) {
            return TFloat32.tensorOf((FloatNdArray) array);
        }
        else if (array instanceof DoubleNdArray) {
            return TFloat64.tensorOf((DoubleNdArray) array);
        }
        else if (array instanceof ByteNdArray) {
            return TUint8.tensorOf((ByteNdArray) array);
        }
        else if (array instanceof BooleanNdArray) {
            return TBool.tensorOf((BooleanNdArray) array);
        }

        throw new RuntimeException("Unsupported NdArray type: " + array.getClass().getName());
    }
}
