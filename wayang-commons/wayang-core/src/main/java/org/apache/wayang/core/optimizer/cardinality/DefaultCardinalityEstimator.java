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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;

import java.util.Arrays;

/**
 * Default implementation of the {@link CardinalityEstimator}. Generalizes a single-point estimation function.
 */
public class DefaultCardinalityEstimator implements CardinalityEstimator {

    private final double certaintyProb;

    private final int numInputs;

    private final FunctionDescriptor.SerializableToLongBiFunction<long[], Configuration> singlePointEstimator;

    /**
     * If {@code true}, receiving more than {@link #numInputs} is also fine.
     */
    private final boolean isAllowMoreInputs;

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       boolean isAllowMoreInputs,
                                       FunctionDescriptor.SerializableToLongFunction<long[]> singlePointEstimator) {
        this(certaintyProb,
                numInputs,
                isAllowMoreInputs,
                (inputCards, wayangContext) -> singlePointEstimator.applyAsLong(inputCards));
    }

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       boolean isAllowMoreInputs,
                                       FunctionDescriptor.SerializableToLongBiFunction<long[], Configuration> singlePointEstimator) {
        this.certaintyProb = certaintyProb;
        this.numInputs = numInputs;
        this.singlePointEstimator = singlePointEstimator;
        this.isAllowMoreInputs = isAllowMoreInputs;
    }


    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        assert inputEstimates.length == this.numInputs
                || (this.isAllowMoreInputs && inputEstimates.length > this.numInputs) :
                String.format("Received %d input estimates, require %d%s.",
                        inputEstimates.length, this.numInputs, this.isAllowMoreInputs ? "+" : "");

        if (this.numInputs == 0) {
            final long estimate = this.singlePointEstimator.applyAsLong(new long[0], optimizationContext.getConfiguration());
            return new CardinalityEstimate(estimate, estimate, this.certaintyProb);
        }

        long[] lowerAndUpperInputEstimates = this.extractEstimateValues(inputEstimates);

        long lowerEstimate = -1, upperEstimate = -1;
        long[] currentInputEstimates = new long[this.numInputs];
        final int maximumBitMaskValue = 1 << this.numInputs;
        for (long positionBitmask = 0; positionBitmask < maximumBitMaskValue; positionBitmask++) {
            for (int pos = 0; pos < this.numInputs; pos++) {
                int bit = (int) ((positionBitmask >>> pos) & 0x1);
                currentInputEstimates[pos] = lowerAndUpperInputEstimates[(pos << 1) + bit];
            }
            long currentEstimate = Math.max(
                    this.singlePointEstimator.applyAsLong(currentInputEstimates, optimizationContext.getConfiguration()),
                    0
            );
            if (lowerEstimate == -1 || currentEstimate < lowerEstimate) {
                lowerEstimate = currentEstimate;
            }
            if (upperEstimate == -1 || currentEstimate > upperEstimate) {
                upperEstimate = currentEstimate;
            }
        }

        double correctnessProb = this.certaintyProb * Arrays.stream(inputEstimates)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .min()
                .orElseThrow(IllegalStateException::new);
        return new CardinalityEstimate(lowerEstimate, upperEstimate, correctnessProb);
    }

    private long[] extractEstimateValues(CardinalityEstimate[] inputEstimates) {
        long[] lowerAndUpperEstimates = new long[this.numInputs * 2];
        for (int i = 0; i < this.numInputs; i++) {
            final CardinalityEstimate inputEstimate = inputEstimates[i];
            lowerAndUpperEstimates[i << 1] = inputEstimate.getLowerEstimate();
            lowerAndUpperEstimates[(i << 1) + 1] = inputEstimate.getUpperEstimate();
        }
        return lowerAndUpperEstimates;
    }
}
