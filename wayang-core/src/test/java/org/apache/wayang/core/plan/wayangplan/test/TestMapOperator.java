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

package org.apache.wayang.core.plan.wayangplan.test;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

import java.util.Optional;

/**
 * Test operator that exposes map-like behavior.
 */
public class TestMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {
    /**
     * Creates a new instance.
     */
    public TestMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
    }

    public TestMapOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        this(
                DataSetType.createDefault(DataUnitType.createBasic(inputTypeClass)),
                DataSetType.createDefault(DataUnitType.createBasic(outputTypeClass))
        );
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        Validate.isTrue(outputIndex == 0);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, true, cards -> cards[0]));
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return true;
    }
}
