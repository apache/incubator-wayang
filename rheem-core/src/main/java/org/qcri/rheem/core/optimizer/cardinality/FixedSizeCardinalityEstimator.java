package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

/**
 * {@link CardinalityEstimator} implementation for {@link Operator}s with a fix-sized output.
 */
public class FixedSizeCardinalityEstimator implements CardinalityEstimator {

    private final long outputSize;

    public FixedSizeCardinalityEstimator(long outputSize) {
        this.outputSize = outputSize;
    }

    @Override
    public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
        return new CardinalityEstimate(this.outputSize, this.outputSize, 1d);
    }
}
