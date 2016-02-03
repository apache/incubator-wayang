package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

public class DefaultCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimator[] cardinalityEstimators;

    public DefaultCardinalityPusher(Operator operator, Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(operator, cache);
        this.cardinalityEstimators = new CardinalityEstimator[operator.getNumOutputs()];
        for (int outputIndex = 0; outputIndex < operator.getNumOutputs(); outputIndex++) {
            initializeEstimator(operator, outputIndex);
        }
    }

    private void initializeEstimator(final Operator operator, final int outputIndex) {
        operator.getCardinalityEstimator(outputIndex, this.cache)
                .ifPresent(estimator -> this.cardinalityEstimators[outputIndex] = estimator);
    }

    @Override
    protected CardinalityEstimate[] doPush(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        CardinalityEstimate[] estimates = new CardinalityEstimate[this.cardinalityEstimators.length];
        for (int outputIndex = 0; outputIndex < this.cardinalityEstimators.length; outputIndex++) {
            final CardinalityEstimator estimator = this.cardinalityEstimators[outputIndex];
            if (estimator != null) {
                estimates[outputIndex] = estimator.estimate(rheemContext, inputEstimates);
            }
        }
        return estimates;
    }
}
