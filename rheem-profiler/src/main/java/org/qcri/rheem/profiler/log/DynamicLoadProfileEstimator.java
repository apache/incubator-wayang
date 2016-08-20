package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

/**
 * Adjustable {@link LoadProfileEstimator} implementation.
 */
public class DynamicLoadProfileEstimator implements LoadProfileEstimator<Individual> {

    private final DynamicLoadEstimator cpuEstimator, ramEstimator, diskEstimator, networkEstimator;

    public DynamicLoadProfileEstimator(DynamicLoadEstimator.SinglePointEstimator cpuEstimator) {
        this(
                cpuEstimator,
                (individual, inputCardinalities, outputCardinalities) -> 0d,
                (individual, inputCardinalities, outputCardinalities) -> 0d
        );
    }

    public DynamicLoadProfileEstimator(DynamicLoadEstimator.SinglePointEstimator cpuEstimator,
                                       DynamicLoadEstimator.SinglePointEstimator diskEstimator,
                                       DynamicLoadEstimator.SinglePointEstimator networkEstimator) {
        this.cpuEstimator = new DynamicLoadEstimator(cpuEstimator);
        this.ramEstimator = new DynamicLoadEstimator(((individual, inputCardinalities, outputCardinalities) -> 0d));
        this.diskEstimator = new DynamicLoadEstimator(diskEstimator);
        this.networkEstimator = new DynamicLoadEstimator(networkEstimator);
    }

    @Override
    public LoadProfile estimate(Individual individual, CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return new LoadProfile(
                this.cpuEstimator.calculate(individual, inputEstimates, outputEstimates),
                this.ramEstimator.calculate(individual, inputEstimates, outputEstimates),
                this.diskEstimator.calculate(individual, inputEstimates, outputEstimates),
                this.networkEstimator.calculate(individual, inputEstimates, outputEstimates)
        );
    }
}
