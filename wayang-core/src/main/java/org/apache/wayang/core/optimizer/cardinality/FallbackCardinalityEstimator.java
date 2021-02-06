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

import org.apache.wayang.core.optimizer.OptimizationContext;

/**
 * Assumes with a confidence of 50% that the output cardinality will be somewhere between 1 and the product of
 * all 10*input estimates.
 */
public class FallbackCardinalityEstimator implements CardinalityEstimator {

    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        double probability = .5d;
        long upperEstimate = 1L;
        for (CardinalityEstimate inputEstimate : inputEstimates) {
            if (inputEstimate == null) {
                inputEstimate = CardinalityEstimate.EMPTY_ESTIMATE;
            }
            probability *= inputEstimate.getCorrectnessProbability();
            upperEstimate *= 1 + 10 * inputEstimate.getUpperEstimate();
            if (upperEstimate < 0L) {
                upperEstimate = Long.MAX_VALUE;
            }
        }
        return new CardinalityEstimate(1L, upperEstimate, probability);
    }

}
