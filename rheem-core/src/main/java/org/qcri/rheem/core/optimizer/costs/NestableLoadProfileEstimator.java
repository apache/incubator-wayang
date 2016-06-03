package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.util.JuelUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.ToLongBiFunction;

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
     * Creates a new instance from a specification {@link String}. Valid specifications are as follows:
     * <pre>
     *     {"cpu":&lt;JUEL expression&gt;,
     *      "ram":&lt;JUEL expression&gt;,
     *      "disk":&lt;JUEL expression&gt;,
     *      "network":&lt;JUEL expression&gt;,
     *      "in":&lt;#inputs&gt;,
     *      "out":&lt;#outputs&gt;,
     *      "p":&lt;correctness probability&gt;,
     *      "overhead":&lt;overhead in milliseconds&gt;,
     *      "ru":&lt;resource utilization&gt;
     *      }
     * </pre>
     * The JUEL expressions accept as parameters {@code in0}, {@code in1} a.s.o. for the input cardinalities and
     * {@code out0}, {@code out1} a.s.o. for the output cardinalities.
     *
     * @param jsonJuelSpec a specification that adheres to above format
     * @return the new instance
     */
    public static NestableLoadProfileEstimator parseSpecification(String jsonJuelSpec) {
        final JSONObject spec = new JSONObject(jsonJuelSpec);
        int numInputs = spec.getInt("in");
        int numOutputs = spec.getInt("out");
        double correctnessProb = spec.getDouble("p");

        LoadEstimator cpuEstimator = new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                parseJuel(spec.getString("cpu"), numInputs, numOutputs)
        );
        LoadEstimator ramEstimator = new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                parseJuel(spec.getString("ram"), numInputs, numOutputs)
        );
        LoadEstimator diskEstimator = !spec.has("disk") ? null : new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                parseJuel(spec.getString("disk"), numInputs, numOutputs)
        );
        LoadEstimator networkEstimator = !spec.has("network") ? null : new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                parseJuel(spec.getString("network"), numInputs, numOutputs)
        );

        long overhead = spec.has("overhead") ? spec.getLong("overhead") : 0L;
        double resourceUtilization = spec.has("ru") ? spec.getDouble("ru") : 1d;
        return new NestableLoadProfileEstimator(
                cpuEstimator,
                ramEstimator,
                diskEstimator,
                networkEstimator,
                resourceUtilization,
                overhead
        );
    }

    private static ToLongBiFunction<long[], long[]> parseJuel(String juel, int numInputs, int numOutputs) {
        final Map<String, Class<?>> parameterClasses = new HashMap<>(numOutputs + numOutputs);
        for (int i = 0; i < numInputs; i++) {
            parameterClasses.put("in" + i, Long.class);
        }
        for (int i = 0; i < numOutputs; i++) {
            parameterClasses.put("out" + i, Long.class);
        }
        final JuelUtils.JuelFunction<Long> juelFunction = new JuelUtils.JuelFunction<>(juel, Long.class, parameterClasses);
        return (inCards, outCards) -> {
            final Map<String, Object> parameters = new HashMap<>(numOutputs + numOutputs);
            for (int i = 0; i < numInputs; i++) {
                parameters.put("in" + i, inCards[i]);
            }
            for (int i = 0; i < numOutputs; i++) {
                parameters.put("out" + i, outCards[i]);
            }
            return juelFunction.apply(parameters);
        };
    }

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
        if (this.getResourceUtilization() < 1d) {
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
