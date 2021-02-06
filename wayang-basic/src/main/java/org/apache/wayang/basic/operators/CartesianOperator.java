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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class CartesianOperator<InputType0, InputType1>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    public CartesianOperator(Class<InputType0> inputType0Class, Class<InputType1> inputType1Class) {
        super(DataSetType.createDefault(inputType0Class),
                DataSetType.createDefault(inputType1Class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                true);
    }

    public CartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1, DataSetType.createDefaultUnchecked(Tuple2.class), true);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CartesianOperator(CartesianOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(
                1d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0] * inputCards[1]
        ));
    }
}
