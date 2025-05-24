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
import org.tensorflow.ConcreteFunction;
import org.tensorflow.Graph;
import org.tensorflow.Operand;
import org.tensorflow.Signature;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.op.Ops;
import org.tensorflow.op.core.Placeholder;
import org.tensorflow.op.core.Variable;
import org.tensorflow.op.nn.FusedBatchNorm;
import org.tensorflow.types.TBool;
import org.tensorflow.types.family.TNumber;

import java.util.Collections;

public class TensorflowBatchNorm2D<T extends TNumber> {
    private final Graph graph;
    private final Ops tf;
    private final BatchNorm2D op;
    private final Class<T> tClass;
    private final Variable<T> weight;
    private final Variable<T> bias;
    private final Variable<T> runningMean;
    private final Variable<T> runningVar;


    public TensorflowBatchNorm2D(Graph graph, Ops tf, BatchNorm2D op, Class<T> tClass) {
        this.graph = graph;
        this.tf = tf;
        this.op = op;
        this.tClass = tClass;
        this.weight = tf.variable(tf.random.truncatedNormal(tf.array(op.getNumFeatures()), tClass));
        this.bias = tf.variable(tf.random.truncatedNormal(tf.array(op.getNumFeatures()), tClass));
        this.runningMean = tf.variable(tf.zeros(tf.array(op.getNumFeatures()), tClass));
        this.runningVar = tf.variable(tf.ones(tf.array(op.getNumFeatures()), tClass));
    }

    public Operand<T> call(Operand<T> input, Operand<TBool> trainingMode) {
        Operand<?> out = tf.withName(op.getName()).ifOp(
                trainingMode,
                Collections.singletonList(input),
                Collections.singletonList(tClass),
                training(), inference()
        ).iterator().next();

        return (Operand<T>) out;
    }

    public ConcreteFunction training() {
        Placeholder<T> trainingInput = tf.withName("trainingInput").placeholder(tClass);

        FusedBatchNorm<T, T> batchNormTraining = tf.nn.fusedBatchNorm(
                trainingInput, weight, bias,
                tf.fakeParam(tClass, Shape.of(op.getNumFeatures())),
                tf.fakeParam(tClass, Shape.of(op.getNumFeatures())),
                FusedBatchNorm.epsilon(op.getEpsilon())
                        .exponentialAvgFactor(op.getMomentum())
                        .dataFormat("NCHW")
                        .isTraining(true)
        );
        Operand<T> mean = tf.math.add(
                tf.math.mul(tf.dtypes.cast(tf.constant(op.getMomentum()), tClass), tf.stopGradient(runningMean)),
                tf.math.mul(tf.dtypes.cast(tf.constant(1f - op.getMomentum()), tClass), batchNormTraining.batchMean())
        );
        Operand<T> var = tf.math.add(
                tf.math.mul(tf.dtypes.cast(tf.constant(op.getMomentum()), tClass), tf.stopGradient(runningVar)),
                tf.math.mul(tf.dtypes.cast(tf.constant(1f - op.getMomentum()), tClass), batchNormTraining.batchVariance())
        );
        Operand<T> y = tf.withControlDependencies( // FIXME: assign is not executed
                tf.assign(runningMean, mean), tf.assign(runningVar, var)
        ).identity(batchNormTraining.y());
        return ConcreteFunction.create(
                Signature.builder().input("x", trainingInput).output("y", y).build(),
                graph
        );
    }

    public ConcreteFunction inference() {
        Placeholder<T> inferenceInput = tf.withName("inferenceInput").placeholder(tClass);
        FusedBatchNorm<T, T> batchNormInference = tf.nn.fusedBatchNorm(
                inferenceInput, weight, bias, runningMean, runningVar,
                FusedBatchNorm.epsilon(op.getEpsilon())
                        .exponentialAvgFactor(op.getMomentum())
                        .dataFormat("NCHW")
                        .isTraining(false)
        );
        return ConcreteFunction.create(
                Signature.builder().input("x", inferenceInput).output("y", batchNormInference.y()).build(),
                graph
        );
    }
}
