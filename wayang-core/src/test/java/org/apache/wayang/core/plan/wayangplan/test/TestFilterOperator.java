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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

import java.util.Optional;

/**
 * Test operator that exposes filter-like behavior.
 */
public class TestFilterOperator<InputType> extends UnaryToUnaryOperator<InputType, InputType> {

    private double selectivity = 0.7d;

    /**
     * Creates a new instance.
     */
    public TestFilterOperator(DataSetType<InputType> inputType) {
        super(inputType, inputType, true);
    }

    public TestFilterOperator(Class<InputType> inputTypeClass) {
        this(DataSetType.createDefault(DataUnitType.createBasic(inputTypeClass)));
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        assert outputIndex == 0;
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, true, cards -> Math.round(this.selectivity * cards[0])));
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return true;
    }

    public double getSelectivity() {
        return this.selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }
}
