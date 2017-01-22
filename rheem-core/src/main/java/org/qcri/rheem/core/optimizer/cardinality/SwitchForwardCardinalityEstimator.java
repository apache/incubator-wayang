package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.slf4j.LoggerFactory;

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
                    LoggerFactory.getLogger(this.getClass()).error("Conflicting estimates {} and {}.", forwardEstimate, inputEstimate);
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
