package io.rheem.rheem.core.optimizer.cardinality;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.configuration.KeyValueProvider;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.OutputSlot;

/**
 * Default {@link CardinalityPusher} implementation. Bundles all {@link CardinalityEstimator}s of an {@link Operator}.
 */
public class DefaultCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimator[] cardinalityEstimators;

    public DefaultCardinalityPusher(Operator operator,
                                    KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        super(operator);
        this.cardinalityEstimators = this.initializeCardinalityEstimators(operator, estimationProvider);
    }

    public DefaultCardinalityPusher(Operator operator,
                                    int[] relevantInputIndices,
                                    int[] relevantOutputIndices,
                                    KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {
        super(relevantInputIndices, relevantOutputIndices);
        this.cardinalityEstimators = this.initializeCardinalityEstimators(operator, estimationProvider);
    }

    /**
     * Initializes the {@link CardinalityEstimator}s required by this instance.
     */
    private CardinalityEstimator[] initializeCardinalityEstimators(
            Operator operator,
            KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimationProvider) {

        final CardinalityEstimator[] cardinalityEstimators = new CardinalityEstimator[operator.getNumOutputs()];
        for (int outputIndex : this.relevantOutputIndices) {
            final CardinalityEstimator estimator = estimationProvider.provideFor(operator.getOutput(outputIndex));
            cardinalityEstimators[outputIndex] = estimator;
        }
        return cardinalityEstimators;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        for (int outputIndex : this.relevantOutputIndices) {
            final CardinalityEstimator estimator = this.cardinalityEstimators[outputIndex];
            if (estimator != null) {
                opCtx.setOutputCardinality(
                        outputIndex,
                        estimator.estimate(opCtx.getOptimizationContext(), opCtx.getInputCardinalities())
                );
            }
        }
    }
}
