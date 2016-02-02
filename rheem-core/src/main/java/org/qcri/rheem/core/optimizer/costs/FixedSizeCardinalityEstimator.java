package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;

/**
 * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.
 */
public class FixedSizeCardinalityEstimator implements CardinalityEstimator {

    private final long outputSize;

    public FixedSizeCardinalityEstimator(long outputSize) {
        this.outputSize = outputSize;
    }

    @Override
    public CardinalityEstimate estimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        return new CardinalityEstimate(this.outputSize, this.outputSize, 1d);
    }
}
