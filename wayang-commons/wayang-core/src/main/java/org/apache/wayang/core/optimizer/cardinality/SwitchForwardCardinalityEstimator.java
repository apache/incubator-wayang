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
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.logging.log4j.LogManager;

/**
 * Forwards the {@link CardinalityEstimate} of any given {@link InputSlot} that is not {@code null}. Asserts that
 * all other {@link CardinalityEstimate}s are indeed {@code null}.
 */
public class SwitchForwardCardinalityEstimator implements CardinalityEstimator {

    private final int[] switchInputIndices;

    public SwitchForwardCardinalityEstimator(int... switchInputIndices) {
        assert switchInputIndices.length > 0;
        this.switchInputIndices = switchInputIndices;
    }

    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        CardinalityEstimate forwardEstimate = null;
        for (int switchInputIndex : this.switchInputIndices) {
            final CardinalityEstimate inputEstimate = inputEstimates[switchInputIndex];
            if (inputEstimate != null) {
                if (forwardEstimate != null) {
                    LogManager.getLogger(this.getClass()).error("Conflicting estimates {} and {}.", forwardEstimate, inputEstimate);
                }
                if (forwardEstimate == null || forwardEstimate.getCorrectnessProbability() > inputEstimate.getCorrectnessProbability()) {
                    forwardEstimate = inputEstimate;
                }
            }
        }
        assert forwardEstimate != null;
        return forwardEstimate;
    }

}
