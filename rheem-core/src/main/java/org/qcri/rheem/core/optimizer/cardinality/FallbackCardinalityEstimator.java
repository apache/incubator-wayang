package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;

/**
 * Assumes with a confidence of 50% that the output cardinality will be somewhere between 1 and the product of
 * all 10*input estimates.
 */
public class FallbackCardinalityEstimator implements CardinalityEstimator {

    @Override
    public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
        double probability = .5d;
        long upperEstimate = 1L;
        for (CardinalityEstimate inputEstimate : inputEstimates) {
            probability *= inputEstimate.getCorrectnessProbability();
            upperEstimate *= 10 * inputEstimate.getUpperEstimate();
            if (upperEstimate < 0L) {
                upperEstimate = Long.MAX_VALUE;
            }
        }
        return new CardinalityEstimate(1L, upperEstimate, probability);
    }

}
