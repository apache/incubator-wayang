package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.configuration.KeyValueProvider;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

/**
 * Default {@link CardinalityPusher} implementation. Bundles all {@link CardinalityEstimator}s of an {@link Operator}.
 */
public class DefaultCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimator[] cardinalityEstimators;

    public DefaultCardinalityPusher(Operator operator,
                                    KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        this.cardinalityEstimators = new CardinalityEstimator[operator.getNumOutputs()];
        for (int outputIndex = 0; outputIndex < operator.getNumOutputs(); outputIndex++) {
            this.initializeEstimator(operator, outputIndex, estimationProvider);
        }
    }

    private void initializeEstimator(final Operator operator, final int outputIndex, KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        final CardinalityEstimator estimator = estimationProvider.provideFor(operator.getOutput(outputIndex));
        this.cardinalityEstimators[outputIndex] = estimator;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        for (int outputIndex = 0; outputIndex < this.cardinalityEstimators.length; outputIndex++) {
            final CardinalityEstimator estimator = this.cardinalityEstimators[outputIndex];
            if (estimator != null) {
                opCtx.setOutputCardinality(outputIndex, estimator.estimate(configuration, opCtx.getInputCardinalities()));
            }
        }
    }
}
