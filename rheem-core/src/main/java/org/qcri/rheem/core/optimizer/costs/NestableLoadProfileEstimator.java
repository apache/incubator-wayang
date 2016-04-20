package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator implements LoadProfileEstimator {

    private final LoadEstimator cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

    /**
     * The degree to which the load profile can utilize available resources.
     */
    private final double resourceUtilization;

    /**
     * Milliseconds overhead that this load profile incurs.
     */
    private final long overheadMillis;

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
        this(cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator, 1d, 0L);
    }

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator     estimates CPU load in terms of cycles
     * @param ramLoadEstimator     estimates RAM load in terms of bytes
     * @param diskLoadEstimator    estimates disk accesses in terms of bytes
     * @param networkLoadEstimator estimates network in terms of bytes
     * @param resourceUtilization  degree to which the load profile can utilize available resources
     * @param overheadMillis       overhead that this load profile incurs
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator,
                                        double resourceUtilization,
                                        long overheadMillis) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
        this.resourceUtilization = resourceUtilization;
        this.overheadMillis = overheadMillis;
    }

    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedLoadEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(OptimizationContext.OperatorContext operatorContext) {
        return this.estimate(operatorContext.getInputCardinalities(), operatorContext.getOutputCardinalities());
    }

    @Override
    public LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadProfile mainLoadProfile = this.performLocalEstimation(inputEstimates, outputEstimates);
        this.performNestedEstimations(inputEstimates, outputEstimates, mainLoadProfile);
        return mainLoadProfile;
    }

    private LoadProfile performLocalEstimation(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ? null :
                this.diskLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ? null :
                this.networkLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadProfile loadProfile = new LoadProfile(cpuLoadEstimate, ramLoadEstimate, networkLoadEstimate, diskLoadEstimate);
        if (this.getOverheadMillis() > 0) {
            loadProfile.setOverheadMillis(this.getOverheadMillis());
        }
        if(this.getResourceUtilization() < 1d) {
            loadProfile.setRatioMachines(this.getResourceUtilization());
        }
        return loadProfile;
    }

    private void performNestedEstimations(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates, LoadProfile mainLoadProfile) {
        for (LoadProfileEstimator nestedLoadEstimator : this.nestedLoadEstimators) {
            final LoadProfile subprofile = nestedLoadEstimator.estimate(inputEstimates, outputEstimates);
            mainLoadProfile.nest(subprofile);
        }
    }

    public long getOverheadMillis() {
        return this.overheadMillis;
    }

    public double getResourceUtilization() {
        return this.resourceUtilization;
    }
}
