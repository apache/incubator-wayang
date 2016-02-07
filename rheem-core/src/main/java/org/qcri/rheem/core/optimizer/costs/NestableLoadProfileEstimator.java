package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator implements LoadProfileEstimator {

    private final LoadEstimator cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

    private Collection<LoadProfileEstimator> nestedLoadEstimators = new LinkedList<>();

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator estimates CPU load in terms of cycles
     * @param ramLoadEstimator estimates RAM load in terms of MB
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator, LoadEstimator ramLoadEstimator) {
        this(cpuLoadEstimator, ramLoadEstimator, null, null);
    }

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator     estimates CPU load in terms of cycles
     * @param ramLoadEstimator     estimates RAM load in terms of bytes
     * @param diskLoadEstimator    estimates disk accesses in terms of bytes
     * @param networkLoadEstimator estimates network in terms of bytes
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
    }

    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedLoadEstimators.add(nestedEstimator);
    }

    public LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadProfile mainLoadProfile = performLocalEstimation(inputEstimates, outputEstimates);
        performNestedEstimations(inputEstimates, outputEstimates, mainLoadProfile);
        return mainLoadProfile;
    }

    private LoadProfile performLocalEstimation(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ? null :
                this.diskLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ? null :
                this.networkLoadEstimator.calculate(inputEstimates, outputEstimates);
        return new LoadProfile(cpuLoadEstimate, ramLoadEstimate, networkLoadEstimate, diskLoadEstimate);
    }

    private void performNestedEstimations(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates, LoadProfile mainLoadProfile) {
        for (LoadProfileEstimator nestedLoadEstimator : this.nestedLoadEstimators) {
            final LoadProfile subprofile = nestedLoadEstimator.estimate(inputEstimates, outputEstimates);
            mainLoadProfile.nest(subprofile);
        }
    }


}
