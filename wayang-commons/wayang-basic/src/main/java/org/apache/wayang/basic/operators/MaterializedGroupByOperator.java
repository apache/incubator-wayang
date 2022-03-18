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
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator collocates the data units in a data set w.r.t. a key function.
 */
public class MaterializedGroupByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Iterable<Type>> {

    protected final TransformationDescriptor<Type, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param keyFunction describes how to extract the key from data units
     * @param typeClass   class of the data quanta to be grouped
     * @param keyClass    class of the extracted keys
     */
    public MaterializedGroupByOperator(FunctionDescriptor.SerializableFunction<Type, Key> keyFunction,
                                       Class<Type> typeClass,
                                       Class<Key> keyClass) {
        this(new TransformationDescriptor<>(keyFunction, typeClass, keyClass));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes how to extract the key from data units
     */
    public MaterializedGroupByOperator(TransformationDescriptor<Type, Key> keyDescriptor) {
        this(keyDescriptor,
                DataSetType.createDefault(keyDescriptor.getInputType()),
                DataSetType.createGrouped(keyDescriptor.getInputType()));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes how to extract the key from data units
     * @param inputType     type of the input elements
     * @param outputType    type of the element groups
     */
    public MaterializedGroupByOperator(TransformationDescriptor<Type, Key> keyDescriptor,
                                       DataSetType<Type> inputType,
                                       DataSetType<Iterable<Type>> outputType) {
        super(inputType, outputType, false);
        this.keyDescriptor = keyDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public MaterializedGroupByOperator(MaterializedGroupByOperator<Type, Key> that) {
        super(that);
        this.keyDescriptor = that.getKeyDescriptor();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new DefaultCardinalityEstimator(0.5d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> (long) (inputCards[0] * 0.1)));
    }

}
