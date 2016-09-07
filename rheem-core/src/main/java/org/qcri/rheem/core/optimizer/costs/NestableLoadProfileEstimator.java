package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.util.Tuple;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.ToDoubleBiFunction;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator<Artifact> implements LoadProfileEstimator<Artifact> {

    /**
     * {@link LoadEstimator} to estimate a certain aspect of the {@link LoadProfile}s for the {@code Artifact}.
     */
    private final LoadEstimator<Artifact> cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

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
    private Collection<Tuple<Function<Artifact, ?>, LoadProfileEstimator<?>>> nestedEstimators = new LinkedList<>();


    /**
     * Creates an new instance.
     *
     * @param cpuLoadEstimator estimates CPU load in terms of cycles
     * @param ramLoadEstimator estimates RAM load in terms of MB
     */
    public NestableLoadProfileEstimator(LoadEstimator<Artifact> cpuLoadEstimator, LoadEstimator<Artifact> ramLoadEstimator) {
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
    public NestableLoadProfileEstimator(LoadEstimator<Artifact> cpuLoadEstimator,
                                        LoadEstimator<Artifact> ramLoadEstimator,
                                        LoadEstimator<Artifact> diskLoadEstimator,
                                        LoadEstimator<Artifact> networkLoadEstimator) {
        this(cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator, (in, out) -> 1d, 0L);
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
    public NestableLoadProfileEstimator(LoadEstimator<Artifact> cpuLoadEstimator,
                                        LoadEstimator<Artifact> ramLoadEstimator,
                                        LoadEstimator<Artifact> diskLoadEstimator,
                                        LoadEstimator<Artifact> networkLoadEstimator,
                                        ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator,
                                        long overheadMillis) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
        this.resourceUtilizationEstimator = resourceUtilizationEstimator;
        this.overheadMillis = overheadMillis;
    }

    /**
     * Nest a {@link LoadProfileEstimator} in this instance. No artifact will be available to it, though.
     *
     * @param nestedEstimator  the {@link LoadProfileEstimator} that should be nested
     * @param <NestedArtifact> the type of sub-artifact
     * @see #nest(LoadProfileEstimator, Function)
     */
    public <NestedArtifact> void nest(LoadProfileEstimator<NestedArtifact> nestedEstimator) {
        this.nest(nestedEstimator, artifact -> null);
    }

    /**
     * Nest a {@link LoadProfileEstimator} in this instance. No artifact will be available to it, though.
     *
     * @param nestedEstimator          the {@link LoadProfileEstimator} that should be nested
     * @param nestedArtifactExtraction from the artifact of this instance, extract the sub-artifact for the {@code nestedEstimator}
     * @param <NestedArtifact>         the type of sub-artifact
     */
    public <NestedArtifact> void nest(LoadProfileEstimator<NestedArtifact> nestedEstimator,
                                      Function<Artifact, NestedArtifact> nestedArtifactExtraction) {
        this.nestedEstimators.add(new Tuple<>(nestedArtifactExtraction, nestedEstimator));
    }

    @Override
    public LoadProfile estimate(Artifact artifact,
                                CardinalityEstimate[] inputCardinalities,
                                CardinalityEstimate[] outputCardinalities) {
        // Estimate the load for the very artifact.
        final LoadProfile mainLoadProfile = this.performLocalEstimation(artifact, inputCardinalities, outputCardinalities);

        // Estiamte the load for any nested artifacts.
        for (Tuple<Function<Artifact, ?>, LoadProfileEstimator<?>> nestedEstimatorDescriptor : this.nestedEstimators) {
            Object nestedArtifact = nestedEstimatorDescriptor.getField0().apply(artifact);
            @SuppressWarnings("unchecked")
            LoadProfileEstimator<Object> nestedEstimator = (LoadProfileEstimator<Object>) nestedEstimatorDescriptor.getField1();
            LoadProfile nestedProfile = nestedEstimator.estimate(nestedArtifact, inputCardinalities, outputCardinalities);
            mainLoadProfile.nest(nestedProfile);
        }

        // Return the complete LoadProfile.
        return mainLoadProfile;
    }

    /**
     * Perform the estimation for the very {@code artifact}, i.e., without any nested artifacts.
     *
     * @param artifact        the subject to estimation
     * @param inputEstimates  input {@link CardinalityEstimate}s for the estimation
     * @param outputEstimates output {@link CardinalityEstimate}s for the estimation
     * @return the {@link LoadProfile} for the {@code artifact}
     */
    private LoadProfile performLocalEstimation(Artifact artifact,
                                               CardinalityEstimate[] inputEstimates,
                                               CardinalityEstimate[] outputEstimates) {
        final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(artifact, inputEstimates, outputEstimates);
        final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(artifact, inputEstimates, outputEstimates);
        final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ?
                null :
                this.diskLoadEstimator.calculate(artifact, inputEstimates, outputEstimates);
        final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ?
                null :
                this.networkLoadEstimator.calculate(artifact, inputEstimates, outputEstimates);
        final double resourceUtilization = this.estimateResourceUtilization(inputEstimates, outputEstimates);
        return new LoadProfile(
                cpuLoadEstimate,
                ramLoadEstimate,
                networkLoadEstimate,
                diskLoadEstimate,
                resourceUtilization,
                this.getOverheadMillis()
        );
    }

    /**
     * Estimates the resource utilization.
     *
     * @param inputEstimates  input {@link CardinalityEstimate}s
     * @param outputEstimates output {@link CardinalityEstimate}s
     * @return the estimated resource utilization
     */
    private double estimateResourceUtilization(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        long[] avgInputEstimates = extractMeanValues(inputEstimates);
        long[] avgOutputEstimates = extractMeanValues(outputEstimates);
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

}
