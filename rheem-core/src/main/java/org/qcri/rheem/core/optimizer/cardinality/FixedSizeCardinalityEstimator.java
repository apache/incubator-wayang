package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

/**
 * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.
 */
public class FixedSizeCardinalityEstimator extends CardinalityEstimator.WithCache {

    private final long outputSize;

    public FixedSizeCardinalityEstimator(long outputSize,
                                         OutputSlot<?> targetOutput,
                                         Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
        super(targetOutput, estimateCache);
        this.outputSize = outputSize;
    }

    @Override
    public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        return new CardinalityEstimate(this.outputSize, this.outputSize, 1d);
    }
}
