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
