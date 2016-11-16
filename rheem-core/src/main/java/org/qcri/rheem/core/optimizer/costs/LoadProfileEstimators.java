package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator.SinglePointEstimationFunction;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JuelUtils;
import org.qcri.rheem.core.util.ReflectionUtils;

import java.util.*;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
     *      "import":&lt;["optional", "operator", "properties"]&gt;,
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
    public static NestableLoadProfileEstimator createFromJuelSpecification(String jsonJuelSpec) {
        try {
            final JSONObject spec = new JSONObject(jsonJuelSpec);
            int numInputs = spec.getInt("in");
            int numOutputs = spec.getInt("out");
            double correctnessProb = spec.getDouble("p");
            List<String> operatorProperties = spec.has("import") ?
                    StreamSupport.stream(spec.optJSONArray("import").spliterator(), false).map(Objects::toString).collect(Collectors.toList()) :
                    Collections.emptyList();


            LoadEstimator cpuEstimator = new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("cpu"), numInputs, numOutputs, operatorProperties)
            );
            LoadEstimator ramEstimator = new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("ram"), numInputs, numOutputs, operatorProperties)
            );
            LoadEstimator diskEstimator = !spec.has("disk") ? null : new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("disk"), numInputs, numOutputs, operatorProperties)
            );
            LoadEstimator networkEstimator = !spec.has("network") ? null : new DefaultLoadEstimator(
                    numInputs,
                    numOutputs,
                    correctnessProb,
                    CardinalityEstimate.EMPTY_ESTIMATE,
                    parseLoadJuel(spec.getString("network"), numInputs, numOutputs, operatorProperties)
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
     * Parses a JUEL expression and provides it as a {@link SinglePointEstimationFunction}.
     *
     * @param juel                 a JUEL expression
     * @param numInputs            the number of inputs of the estimated operator, reflected as JUEL variables {@code in0}, {@code in1}, ...
     * @param numOutputs           the number of outputs of the estimated operator, reflected as JUEL variables {@code out0}, {@code out1}, ...
     * @param additionalProperties additional properties to consider
     * @return a {@link ToLongBiFunction} wrapping the JUEL expression
     */
    private static SinglePointEstimationFunction parseLoadJuel(String juel,
                                                               int numInputs,
                                                               int numOutputs,
                                                               List<String> additionalProperties) {
        final Map<String, Class<?>> parameterClasses = createJuelParameterClasses(
                numInputs,
                numOutputs,
                additionalProperties.toArray(new String[additionalProperties.size()])
        );
        final JuelUtils.JuelFunction<Long> juelFunction = new JuelUtils.JuelFunction<>(juel, Long.class, parameterClasses);
        return (op, inCards, outCards) -> applyJuelFunction(juelFunction, op, inCards, outCards, additionalProperties);
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
        return (inCards, outCards) -> applyJuelFunction(juelFunction, null, inCards, outCards, Collections.emptyList());
    }

    /**
     * Creates parameters classes for JUEL expressions based on input and output cardinalities.
     *
     * @param numInputs  the number of inputs
     * @param numOutputs the number of ouputs
     * @return the parameter names mapped to their parameter classes
     */
    private static Map<String, Class<?>> createJuelParameterClasses(int numInputs, int numOutputs, String... additionalProperties) {
        final Map<String, Class<?>> parameterClasses = new HashMap<>(numOutputs + numOutputs);
        for (int i = 0; i < numInputs; i++) {
            parameterClasses.put("in" + i, Long.class);
        }
        for (int i = 0; i < numOutputs; i++) {
            parameterClasses.put("out" + i, Long.class);
        }
        for (String property : additionalProperties) {
            parameterClasses.put(property, Double.class);
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
    private static <T> T applyJuelFunction(JuelUtils.JuelFunction<T> juelFunction,
                                           Object artifact,
                                           long[] inputCardinalities,
                                           long[] outputCardinalities,
                                           List<String> artifactProperties) {
        final Map<String, Object> parameters = new HashMap<>(inputCardinalities.length + outputCardinalities.length);
        for (int i = 0; i < inputCardinalities.length; i++) {
            parameters.put("in" + i, inputCardinalities[i]);
        }
        for (int i = 0; i < outputCardinalities.length; i++) {
            parameters.put("out" + i, outputCardinalities[i]);
        }
        for (String property : artifactProperties) {
            parameters.put(property, ReflectionUtils.getProperty(artifact, property));
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
            LoadProfileEstimator estimator) {

        // Estimate the LoadProfile for that single execution.
        final LoadProfile baseProfile = estimator.estimate(operatorContext.getNormalizedEstimationContext());
        return baseProfile.timesSequential(operatorContext.getNumExecutions());
    }

    /**
     * Utility to nest the {@link LoadProfileEstimator} of a {@link FunctionDescriptor}.
     *
     * @param mainEstimator      an optional {@link LoadProfileEstimator}; should be a {@link NestableLoadProfileEstimator}
     * @param functionDescriptor whose {@link LoadProfileEstimator} should be nested
     * @param configuration      provides the UDF {@link LoadProfileEstimator}
     */
    public static void nestUdfEstimator(Optional<LoadProfileEstimator> mainEstimatorOpt,
                                        FunctionDescriptor functionDescriptor,
                                        Configuration configuration) {
        final LoadProfileEstimator mainEstimator = mainEstimatorOpt.orElse(null);
        if (mainEstimator == null || !(mainEstimator instanceof NestableLoadProfileEstimator)) return;
        final LoadProfileEstimator subestimator = configuration
                .getFunctionLoadProfileEstimatorProvider()
                .provideFor(functionDescriptor);
        ((NestableLoadProfileEstimator) mainEstimator).nest(subestimator);

    }


    private static final ToDoubleBiFunction<long[], long[]> DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR = (in, out) -> 1d;

}
