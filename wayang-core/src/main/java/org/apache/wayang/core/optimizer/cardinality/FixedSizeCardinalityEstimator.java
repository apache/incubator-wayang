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
import org.apache.wayang.core.plan.wayangplan.Operator;

/**
 * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.
 */
public class FixedSizeCardinalityEstimator implements CardinalityEstimator {

    private final long outputSize;

    private final boolean isOverride;

    public FixedSizeCardinalityEstimator(long outputSize) {
        this(outputSize, false);
    }

    public FixedSizeCardinalityEstimator(long outputSize, boolean isOverride) {
        this.outputSize = outputSize;
        this.isOverride = isOverride;
    }

    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        return new CardinalityEstimate(this.outputSize, this.outputSize, 1d, this.isOverride);
    }
}
