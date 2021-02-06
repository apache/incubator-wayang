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
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

import java.util.Optional;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    private CardinalityEstimator cardinalityEstimator = new FixedSizeCardinalityEstimator(100);

    public TestSource(DataSetType outputType) {
        super(outputType);
    }

    public TestSource(Class<?> dataQuantumClass) {
        this(DataSetType.createDefault(DataUnitType.createBasic(dataQuantumClass)));
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        return Optional.ofNullable(this.cardinalityEstimator);
    }

    public void setCardinalityEstimators(CardinalityEstimator cardinalityEstimators) {
        this.cardinalityEstimator = cardinalityEstimators;
    }
}
