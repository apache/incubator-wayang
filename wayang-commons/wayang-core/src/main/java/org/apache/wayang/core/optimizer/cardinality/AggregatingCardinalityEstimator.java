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

package org.apache.wayang.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.optimizer.OptimizationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CardinalityEstimator} implementation that can have multiple ways of calculating a {@link CardinalityEstimate}.
 */
public class AggregatingCardinalityEstimator implements CardinalityEstimator {

    private final List<CardinalityEstimator> alternativeEstimators;

    public AggregatingCardinalityEstimator(List<CardinalityEstimator> alternativeEstimators) {
        Validate.isTrue(!alternativeEstimators.isEmpty());
        this.alternativeEstimators = new ArrayList<>(alternativeEstimators);
    }

    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        // Simply use the estimate with the highest correctness probability.
        // TODO: Check if this is a good way. There are other palpable approaches (e.g., weighted average).
        return this.alternativeEstimators.stream()
                .map(alternativeEstimator -> alternativeEstimator.estimate(optimizationContext, inputEstimates))
                .sorted((estimate1, estimate2) ->
                        Double.compare(estimate2.getCorrectnessProbability(), estimate1.getCorrectnessProbability()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);
    }
}
