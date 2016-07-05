package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.util.JuelUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;

/**
 * {@link LoadProfileEstimator} that can host further {@link LoadProfileEstimator}s.
 */
public class NestableLoadProfileEstimator implements LoadProfileEstimator {

    private static final ToDoubleBiFunction<long[], long[]> DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR = (in, out) -> 1d;

    private final LoadEstimator cpuLoadEstimator, ramLoadEstimator, diskLoadEstimator, networkLoadEstimator;

    /**
     * The degree to which the load profile can utilize available resources.
     */
    private final ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator;

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
     *      "ru":&lt;resource utilization JUEL expression&gt;
     *      }
     * </pre>
     * The JUEL expressions accept as parameters {@code in0}, {@code in1} a.s.o. for the input cardinalities and
     * {@code out0}, {@code out1} a.s.o. for the output cardinalities.
     *
     * @param jsonJuelSpec a specification that adheres to above format
     * @return the new instance
     */
    public static NestableLoadProfileEstimator parseSpecification(String jsonJuelSpec) {
        try {
            final JSONObject spec = new JSONObject(jsonJuelSpec);
            int numInputs = spec.getInt("in");
            int numOutputs = spec.getInt("out");
            double correctnessProb = spec.getDouble("p");

            LoadEstimator cpuEstimator = new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("cpu"), numInputs, numOutputs)
            );
            LoadEstimator ramEstimator = new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("ram"), numInputs, numOutputs)
            );
            LoadEstimator diskEstimator = !spec.has("disk") ? null : new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("disk"), numInputs, numOutputs)
            );
            LoadEstimator networkEstimator = !spec.has("network") ? null : new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("network"), numInputs, numOutputs)
            );

            long overhead = spec.has("overhead") ? spec.getLong("overhead") : 0L;
            ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator = spec.has("ru") ?
                    parseResourceUsageJuel(spec.getString("ru"), numInputs, numOutputs) :
                    DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR;
            return new NestableLoadProfileEstimator(
                    cpuEstimator,
                    ramEstimator,
                    diskEstimator,
                    networkEstimator,
                    resourceUtilizationEstimator,
                    overhead
            );
        } catch (Exception e) {
            throw new RheemException(String.format("Could not initialize from specification \"%s\".", jsonJuelSpec), e);
        }
    }

    /**
     * Parses a JUEL expression and provides it as a {@link ToLongBiFunction}.
     *
     * @param juel       a JUEL expression
     * @param numInputs  the number of inputs of the estimated operator, reflected as JUEL variables {@code in0}, {@code in1}, ...
     * @param numOutputs the number of outputs of the estimated operator, reflected as JUEL variables {@code out0}, {@code out1}, ...
     * @return a {@link ToLongBiFunction} wrapping the JUEL expression
     */
    private static ToLongBiFunction<long[], long[]> parseLoadJuel(String juel, int numInputs, int numOutputs) {
        final Map<String, Class<?>> parameterClasses = createJuelParameterClasses(numInputs, numOutputs);
        final JuelUtils.JuelFunction<Long> juelFunction = new JuelUtils.JuelFunction<>(juel, Long.class, parameterClasses);
        return (inCards, outCards) -> applyJuelFunction(juelFunction, inCards, outCards);
    }

    /**
     * Parses a JUEL expression and provides it as a {@link ToLongBiFunction}.
     *
     * @param juel       a JUEL expression
     * @param numInputs  the number of inputs of the estimated operator, reflected as JUEL variables {@code in0}, {@code in1}, ...
     * @param numOutputs the number of outputs of the estimated operator, reflected as JUEL variables {@code out0}, {@code out1}, ...
     * @return a {@link ToLongBiFunction} wrapping the JUEL expression
     */
    private static ToDoubleBiFunction<long[], long[]> parseResourceUsageJuel(String juel, int numInputs, int numOutputs) {
        final Map<String, Class<?>> parameterClasses = createJuelParameterClasses(numInputs, numOutputs);
        final JuelUtils.JuelFunction<Double> juelFunction = new JuelUtils.JuelFunction<>(juel, Double.class, parameterClasses);
        return (inCards, outCards) -> applyJuelFunction(juelFunction, inCards, outCards);
    }

    /**
     * Creates parameters classes for JUEL expressions based on input and output cardinalities.
     *
     * @param numInputs  the number of inputs
     * @param numOutputs the number of ouputs
     * @return the parameter names mapped to their parameter classes
     */
    private static Map<String, Class<?>> createJuelParameterClasses(int numInputs, int numOutputs) {
        final Map<String, Class<?>> parameterClasses = new HashMap<>(numOutputs + numOutputs);
        for (int i = 0; i < numInputs; i++) {
            parameterClasses.put("in" + i, Long.class);
        }
        for (int i = 0; i < numOutputs; i++) {
            parameterClasses.put("out" + i, Long.class);
        }
        return parameterClasses;
    }

    /**
     * Evaluates a {@link JuelUtils.JuelFunction} with the given {@code inputCardinalities} and {@code outputCardinalities} as parameters.
     *
     * @param juelFunction        the JUEL function to be executed
     * @param inputCardinalities  the input cardinalities
     * @param outputCardinalities the output cardinalities
     * @return the JUEL function result
     */
    private static <T> T applyJuelFunction(JuelUtils.JuelFunction<T> juelFunction, long[] inputCardinalities, long[] outputCardinalities) {
        final Map<String, Object> parameters = new HashMap<>(inputCardinalities.length + outputCardinalities.length);
        for (int i = 0; i < inputCardinalities.length; i++) {
            parameters.put("in" + i, inputCardinalities[i]);
        }
        for (int i = 0; i < outputCardinalities.length; i++) {
            parameters.put("out" + i, outputCardinalities[i]);
        }
        return juelFunction.apply(parameters, true);
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
    public NestableLoadProfileEstimator(LoadEstimator cpuLoadEstimator,
                                        LoadEstimator ramLoadEstimator,
                                        LoadEstimator diskLoadEstimator,
                                        LoadEstimator networkLoadEstimator,
                                        ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator,
                                        long overheadMillis) {
        this.cpuLoadEstimator = cpuLoadEstimator;
        this.ramLoadEstimator = ramLoadEstimator;
        this.diskLoadEstimator = diskLoadEstimator;
        this.networkLoadEstimator = networkLoadEstimator;
        this.resourceUtilizationEstimator = resourceUtilizationEstimator;
        this.overheadMillis = overheadMillis;
    }

    public void nest(LoadProfileEstimator nestedEstimator) {
        this.nestedLoadEstimators.add(nestedEstimator);
    }

    @Override
    public LoadProfile estimate(OptimizationContext.OperatorContext operatorContext) {
        return this.estimate(
                operatorContext.getInputCardinalities(),
                operatorContext.getOutputCardinalities(),
                operatorContext.getNumExecutions()
        );
    }

    @Override
    public LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates, int numExecutions) {
        CardinalityEstimate[] normalizedInputEstimates = normalize(inputEstimates, numExecutions);
        CardinalityEstimate[] normalizedOutputEstimates = normalize(outputEstimates, numExecutions);
        final LoadProfile mainLoadProfile = this.performLocalEstimation(normalizedInputEstimates, normalizedOutputEstimates);
        this.performNestedEstimations(normalizedInputEstimates, normalizedOutputEstimates, mainLoadProfile);
        return mainLoadProfile.timesSequential(numExecutions);
    }

    /**
     * Normalize the given estimates by dividing them by a number of executions.
     *
     * @param estimates     that should be normalized
     * @param numExecutions the number execution
     * @return the normalized estimates (and {@code estimates} if {@code numExecution == 1}
     */
    private static CardinalityEstimate[] normalize(CardinalityEstimate[] estimates, int numExecutions) {
        if (numExecutions == 1 || estimates.length == 0) return estimates;

        CardinalityEstimate[] normalizedEstimates = new CardinalityEstimate[estimates.length];
        for (int i = 0; i < estimates.length; i++) {
            final CardinalityEstimate estimate = estimates[i];
            if (estimate != null) normalizedEstimates[i] = estimate.divideBy(numExecutions);
        }

        return normalizedEstimates;
    }

    private LoadProfile performLocalEstimation(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        final LoadEstimate cpuLoadEstimate = this.cpuLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate ramLoadEstimate = this.ramLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate diskLoadEstimate = this.diskLoadEstimator == null ? null :
                this.diskLoadEstimator.calculate(inputEstimates, outputEstimates);
        final LoadEstimate networkLoadEstimate = this.networkLoadEstimator == null ? null :
                this.networkLoadEstimator.calculate(inputEstimates, outputEstimates);
        final double resourceUtilization = this.estimateResourceUtilization(inputEstimates, outputEstimates);
        return new LoadProfile(
                cpuLoadEstimate,
                ramLoadEstimate,
                networkLoadEstimate,
                diskLoadEstimate,
                resourceUtilization,
                this.getOverheadMillis());
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

    private void performNestedEstimations(CardinalityEstimate[] normalizedInputEstimates,
                                          CardinalityEstimate[] normalizedOutputEstimates,
                                          LoadProfile mainLoadProfile) {
        for (LoadProfileEstimator nestedLoadEstimator : this.nestedLoadEstimators) {
            final LoadProfile subprofile = nestedLoadEstimator.estimate(normalizedInputEstimates, normalizedOutputEstimates, 1);
            mainLoadProfile.nest(subprofile);
        }
    }

    public long getOverheadMillis() {
        return this.overheadMillis;
    }

}
