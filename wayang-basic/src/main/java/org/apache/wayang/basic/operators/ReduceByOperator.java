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

package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class ReduceByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final TransformationDescriptor<Type, Key> keyDescriptor;

    protected final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     */
    public ReduceByOperator(FunctionDescriptor.SerializableFunction<Type, Key> keyFunction,
                            FunctionDescriptor.SerializableBinaryOperator<Type> reduceDescriptor,
                            Class<Key> keyClass,
                            Class<Type> typeClass) {
        this(new TransformationDescriptor<>(keyFunction, typeClass, keyClass),
                new ReduceDescriptor<>(reduceDescriptor, typeClass));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public ReduceByOperator(TransformationDescriptor<Type, Key> keyDescriptor,
                            ReduceDescriptor<Type> reduceDescriptor) {
        this(keyDescriptor, reduceDescriptor, DataSetType.createDefault(keyDescriptor.getInputType()));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     */
    public ReduceByOperator(TransformationDescriptor<Type, Key> keyDescriptor,
                            ReduceDescriptor<Type> reduceDescriptor,
                            DataSetType<Type> type) {
        super(type, type, true);
        this.keyDescriptor = keyDescriptor;
        this.reduceDescriptor = reduceDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ReduceByOperator(ReduceByOperator<Type, Key> that) {
        super(that);
        this.keyDescriptor = that.getKeyDescriptor();
        this.reduceDescriptor = that.getReduceDescriptor();
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return this.reduceDescriptor;
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new DefaultCardinalityEstimator(
                0.5d,
                1,
                this.isSupportingBroadcastInputs(),
                inputCards -> (long) (inputCards[0] * 0.1)));
    }
}
