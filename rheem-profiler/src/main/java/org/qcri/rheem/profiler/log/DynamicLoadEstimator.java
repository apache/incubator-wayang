package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

/**
 * Adjustable {@link LoadProfileEstimator} implementation.
 */
public class DynamicLoadEstimator extends LoadEstimator<Individual> {

    @FunctionalInterface
    public interface SinglePointEstimator {

        double estimate(Individual individual, long[] inputCardinalities, long[] outputCardinalities);

    }

    public DynamicLoadEstimator(SinglePointEstimator singlePointEstimator) {
        super(CardinalityEstimate.EMPTY_ESTIMATE);
        this.singlePointEstimator = singlePointEstimator;
    }

    private final SinglePointEstimator singlePointEstimator;

    @Override
    public LoadEstimate calculate(Individual individual,
                                  CardinalityEstimate[] inputEstimates,
                                  CardinalityEstimate[] outputEstimates) {
        long[] inputCardinalities = new long[inputEstimates.length];
        long[] outputCardinalities = new long[outputEstimates.length];
        for (int i = 0; i < inputEstimates.length; i++) {
            inputCardinalities[i] = this.replaceNullCardinality(inputEstimates[i]).getLowerEstimate();
        }
        for (int i = 0; i < outputEstimates.length; i++) {
            outputCardinalities[i] = this.replaceNullCardinality(outputEstimates[i]).getLowerEstimate();
        }
        double lowerEstimate = this.singlePointEstimator.estimate(individual, inputCardinalities, outputCardinalities);
        for (int i = 0; i < inputEstimates.length; i++) {
            inputCardinalities[i] = this.replaceNullCardinality(inputEstimates[i]).getUpperEstimate();
        }
        for (int i = 0; i < outputEstimates.length; i++) {
            outputCardinalities[i] = this.replaceNullCardinality(outputEstimates[i]).getUpperEstimate();
        }
        double upperEstimate = this.singlePointEstimator.estimate(individual, inputCardinalities, outputCardinalities);
        return new LoadEstimate(
                Math.round(lowerEstimate),
                Math.round(upperEstimate),
                this.calculateJointProbability(inputEstimates, outputEstimates)
        );
    }

}
