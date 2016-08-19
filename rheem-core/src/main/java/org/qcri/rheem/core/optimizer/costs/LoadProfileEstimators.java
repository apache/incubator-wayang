package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JuelUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;

/**
 * Utilities to deal with {@link LoadProfileEstimator}s.
 */
public class LoadProfileEstimators {

    /**
     * Prevent instantiation of this class.
     */
    private LoadProfileEstimators() {
    }

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
    public static <T> NestableLoadProfileEstimator<T> createFromJuelSpecification(String jsonJuelSpec) {
        try {
            final JSONObject spec = new JSONObject(jsonJuelSpec);
            int numInputs = spec.getInt("in");
            int numOutputs = spec.getInt("out");
            double correctnessProb = spec.getDouble("p");

            LoadEstimator<T> cpuEstimator = new DefaultLoadEstimator<>(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("cpu"), numInputs, numOutputs)
            );
            LoadEstimator<T> ramEstimator = new DefaultLoadEstimator<>(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("ram"), numInputs, numOutputs)
            );
            LoadEstimator<T> diskEstimator = !spec.has("disk") ? null : new DefaultLoadEstimator<>(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("disk"), numInputs, numOutputs)
            );
            LoadEstimator<T> networkEstimator = !spec.has("network") ? null : new DefaultLoadEstimator<>(
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
            return new NestableLoadProfileEstimator<>(
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
     * Estimate the {@link LoadProfile} for an {@link OptimizationContext.OperatorContext} using a
     * {@link LoadProfileEstimator} for the corresponding {@link ExecutionOperator}.
     *
     * @param operatorContext the {@link OptimizationContext.OperatorContext}
     * @param estimator       the {@link LoadProfileEstimator} for the {@link ExecutionOperator}
     * @param <T>             the type of the {@link ExecutionOperator} that is in the {@link OptimizationContext.OperatorContext}
     * @return the {@link LoadProfile}
     */
    public static <T extends ExecutionOperator> LoadProfile estimateLoadProfile(
            OptimizationContext.OperatorContext operatorContext,
            LoadProfileEstimator<T> estimator) {

        // Retrieve and normalize the CardinalityEstimates for a single execution.
        CardinalityEstimate[] normalizedInputEstimates = normalize(operatorContext.getInputCardinalities(), operatorContext.getNumExecutions());
        CardinalityEstimate[] normalizedOutputEstimates = normalize(operatorContext.getOutputCardinalities(), operatorContext.getNumExecutions());

        // Estimate the LoadProfile for that single execution.
        @SuppressWarnings("unchecked")
        final LoadProfile baseProfile = estimator.estimate((T) operatorContext.getOperator(), normalizedInputEstimates, normalizedOutputEstimates);
        return baseProfile.timesSequential(operatorContext.getNumExecutions());
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

    private static final ToDoubleBiFunction<long[], long[]> DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR = (in, out) -> 1d;

}
