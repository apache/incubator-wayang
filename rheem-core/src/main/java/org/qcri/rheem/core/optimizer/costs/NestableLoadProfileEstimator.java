package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.ToDoubleBiFunction;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator implements LoadProfileEstimator {

    /**
     * {@link LoadEstimator} to estimate a certain aspect of the {@link LoadProfile}s for the {@code Artifact}.
     */
    private final LoadEstimator cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

    /**
     * The degree to which the load profile can utilize available resources.
     */
    private final ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator;

    /**
     * Milliseconds overhead that this load profile incurs.
     */
    private final long overheadMillis;

    /**
     * Nested {@link LoadProfileEstimator}s together with a {@link Function} to extract the estimated
     * {@code Artifact} from the {@code Artifact}s subject to this instance.
     */
    private Collection<LoadProfileEstimator> nestedEstimators = new LinkedList<>();

    /**
     * If this instance was created from a specification in the {@link Configuration},
     * then the according {@link Configuration} key should be stored.
     */
    private final String configurationKey;

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
        this(cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator, (in, out) -> 1d, 0L, null);
    }

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator             estimates CPU load in terms of cycles
     * @param ramLoadEstimator             estimates RAM load in terms of bytes
     * @param diskLoadEstimator            estimates disk accesses in terms of bytes
     * @param networkLoadEstimator         estimates network in terms of bytes
     * @param resourceUtilizationEstimator degree to which the load profile can utilize available resources
     * @param overheadMillis               overhead that this load profile incurs
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator,
                                        ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator,
                                        long overheadMillis) {
        this(
                cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator,
                resourceUtilizationEstimator, overheadMillis, null
        );
    }

    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator             estimates CPU load in terms of cycles
     * @param ramLoadEstimator             estimates RAM load in terms of bytes
     * @param diskLoadEstimator            estimates disk accesses in terms of bytes
     * @param networkLoadEstimator         estimates network in terms of bytes
     * @param resourceUtilizationEstimator degree to which the load profile can utilize available resources
     * @param overheadMillis               overhead that this load profile incurs
     * @param configurationKey             from that this instances was perceived
     */
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator,
                                        ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator,
                                        long overheadMillis,
                                        String configurationKey) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
        this.resourceUtilizationEstimator = resourceUtilizationEstimator;
        this.overheadMillis = overheadMillis;
        this.configurationKey = configurationKey;
    }

    /**
     * Nest a {@link LoadProfileEstimator} in this instance. No artifact will be available to it, though.
     *
     * @param nestedEstimator the {@link LoadProfileEstimator} that should be nested
     */
    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(EstimationContext context) {
        // Estimate the load for the very artifact.
        final LoadProfile mainLoadProfile = this.performLocalEstimation(context);

        // Estiamte the load for any nested artifacts.
        for (LoadProfileEstimator nestedEstimator : this.nestedEstimators) {
            LoadProfile nestedProfile = nestedEstimator.estimate(context);
            mainLoadProfile.nest(nestedProfile);
        }

        // Return the complete LoadProfile.
        return mainLoadProfile;
    }

    /**
     * Perform the estimation for the very {@code artifact}, i.e., without any nested artifacts.
     *
     * @param context provides parameters for the estimation
     * @return the {@link LoadProfile} for the {@code artifact}
     */
    private LoadProfile performLocalEstimation(EstimationContext context) {
        try {
            final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(context);
            final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(context);
            final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ?
                    null :
                    this.diskLoadEstimator.calculate(context);
            final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ?
                    null :
                    this.networkLoadEstimator.calculate(context);
            final double resourceUtilization = this.estimateResourceUtilization(context);
            return new LoadProfile(
                    cpuLoadEstimate,
                    ramLoadEstimate,
                    networkLoadEstimate,
                    diskLoadEstimate,
                    resourceUtilization,
                    this.getOverheadMillis()
            );
        } catch (Exception e) {
             throw new RheemException(String.format("Failed estimating on %s.", this, context), e);
        }
    }

    /**
     * Estimates the resource utilization.
     *
     * @param context provides parameters for the estimation
     * @return the estimated resource utilization
     */
    private double estimateResourceUtilization(EstimationContext context) {
        long[] avgInputEstimates = extractMeanValues(context.getInputCardinalities());
        long[] avgOutputEstimates = extractMeanValues(context.getOutputCardinalities());
        return this.resourceUtilizationEstimator.applyAsDouble(avgInputEstimates, avgOutputEstimates);
    }

    /**
     * Extracts the geometric mean values of the given {@link CardinalityEstimate}s.
     *
     * @param estimates the input {@link CardinalityEstimate}s
     * @return an array containing the average estimates
     * @see CardinalityEstimate#getGeometricMeanEstimate()
     */
    private static long[] extractMeanValues(CardinalityEstimate[] estimates) {
        long[] averages = new long[estimates.length];
        for (int i = 0; i < estimates.length; i++) {
            CardinalityEstimate inputEstimate = estimates[i];
            if (inputEstimate == null) inputEstimate = CardinalityEstimate.EMPTY_ESTIMATE;
            averages[i] = inputEstimate.getGeometricMeanEstimate();
        }
        return averages;
    }

    private long getOverheadMillis() {
        return this.overheadMillis;
    }

    @Override
    public Collection<LoadProfileEstimator> getNestedEstimators() {
        return this.nestedEstimators;
    }

    @Override
    public String getConfigurationKey() {
        return this.configurationKey;
    }

    @Override
    public LoadProfileEstimator copy() {
        LoadProfileEstimator copy = new NestableLoadProfileEstimator(
                this.cpuLoadEstimator, this.ramLoadEstimator, this.diskLoadEstimator, this.networkLoadEstimator,
                this.resourceUtilizationEstimator, this.overheadMillis, this.configurationKey
        );
        for (LoadProfileEstimator nestedEstimator : this.nestedEstimators) {
            copy.nest(nestedEstimator.copy());
        }
        return copy;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]",
                this.getClass().getSimpleName(),
                this.configurationKey == null ? "(no key)" : this.configurationKey
        );
    }
}
