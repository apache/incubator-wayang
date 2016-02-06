package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.configuration.ConfigurationProvider;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Map;

public class DefaultCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimator[] cardinalityEstimators;

    public DefaultCardinalityPusher(Operator operator,
                                    ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider,
                                    Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(operator, cache);
        this.cardinalityEstimators = new CardinalityEstimator[operator.getNumOutputs()];
        for (int outputIndex = 0; outputIndex < operator.getNumOutputs(); outputIndex++) {
            initializeEstimator(operator, outputIndex, estimationProvider);
        }
    }

    private void initializeEstimator(final Operator operator, final int outputIndex, ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        final CardinalityEstimator estimator = estimationProvider.provideFor(operator.getOutput(outputIndex));
        this.cardinalityEstimators[outputIndex] = estimator;
    }

    @Override
    protected CardinalityEstimate[] doPush(Configuration configuration, CardinalityEstimate... inputEstimates) {
        CardinalityEstimate[] estimates = new CardinalityEstimate[this.cardinalityEstimators.length];
        for (int outputIndex = 0; outputIndex < this.cardinalityEstimators.length; outputIndex++) {
            final CardinalityEstimator estimator = this.cardinalityEstimators[outputIndex];
            if (estimator != null) {
                estimates[outputIndex] = estimator.estimate(configuration, inputEstimates);
            }
        }
        return estimates;
    }
}
