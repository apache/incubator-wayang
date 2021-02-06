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
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator returns the set intersection of elements of input datasets.
 */
public class IntersectOperator<Type> extends BinaryToUnaryOperator<Type, Type, Type> {

    public IntersectOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass));
    }

    public IntersectOperator(DataSetType<Type> dataSetType) {
        super(dataSetType, dataSetType, dataSetType, false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public IntersectOperator(IntersectOperator<Type> that) {
        super(that);
    }

    /**
     * Provides the type of input and output datasets.
     *
     * @return the {@link DataSetType}
     */
    public DataSetType<Type> getType() {
        return this.getInputType0();
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // The current idea: We assume that one side is duplicate-free and included in the other.
        // TODO: Find a better estimator.
        return Optional.of(new DefaultCardinalityEstimator(
                .5d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> Math.min(inputCards[0], inputCards[1])
        ));
    }
}
