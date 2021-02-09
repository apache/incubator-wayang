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

package org.apache.wayang.core.plan.wayangplan;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;

import java.util.Optional;

/**
 * Indivisible {@link Operator} that is not containing other {@link Operator}s.
 */
public interface ElementaryOperator extends ActualOperator {

    /**
     * Provide a {@link CardinalityEstimator} for the {@link OutputSlot} at {@code outputIndex}.
     *
     * @param outputIndex   index of the {@link OutputSlot} for that the {@link CardinalityEstimator} is requested
     * @param configuration if the {@link CardinalityEstimator} depends on further ones, use this to obtain the latter
     * @return an {@link Optional} that might provide the requested instance
     */
    default Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.empty();
    }

    /**
     * Retrieve a {@link CardinalityEstimator} tied specifically to this instance.
     *
     * @param outputIndex for the output described by the {@code cardinalityEstimator}
     * @return the {@link CardinalityEstimator} or {@code null} if none exists
     */
    CardinalityEstimator getCardinalityEstimator(int outputIndex);

    /**
     * Tie a specific {@link CardinalityEstimator} to this instance.
     *
     * @param outputIndex          for the output described by the {@code cardinalityEstimator}
     * @param cardinalityEstimator the {@link CardinalityEstimator}
     */
    void setCardinalityEstimator(int outputIndex, CardinalityEstimator cardinalityEstimator);

    /**
     * Tells whether this instance is auxiliary, i.e., it support some non-auxiliary operators.
     *
     * @return whether this instance is auxiliary
     */
    boolean isAuxiliary();

    /**
     * Tell whether this instance is auxiliary, i.e., it support some non-auxiliary operators.
     *
     * @param isAuxiliary whether this instance is auxiliary
     */
    void setAuxiliary(boolean isAuxiliary);

}
