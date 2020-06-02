package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator.SinglePointEstimationFunction;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JuelUtils;
import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.DefaultContext;
import org.qcri.rheem.core.util.mathex.Expression;
import org.qcri.rheem.core.util.mathex.ExpressionBuilder;
import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Utilities to deal with {@link LoadProfileEstimator}s.
 */
public class LoadProfileEstimators {

    /**
     * Base {@link Context} that provides several functions for estimation purposes.
     */
    public static final Context baseContext;

    private static final Logger logger = LoggerFactory.getLogger(LoadProfileEstimators.class);

    static {
        DefaultContext ctx = new DefaultContext(Context.baseContext);
        ctx.setFunction(
                "logGrowth",
                vals -> OptimizationUtils.logisticGrowth(vals[0], vals[1], vals[2], vals[3])
        );
        baseContext = ctx;
    }

    /**
     * Prevent instantiation of this class.
     */
    private LoadProfileEstimators() {
    }

    /**
     * Creates an {@link LoadProfileEstimator} via a specification that is retrieved from a {@link Configuration}.
     *
     * @param configKey     to look up the specification
     * @param configuration provides the specification and caches the {@link LoadProfileEstimator}
     * @return the {@link LoadProfileEstimator} or {@code null} if no specification was found
     */
    public static LoadProfileEstimator createFromSpecification(String configKey, Configuration configuration) {
        final LoadProfileEstimator cachedEstimator =
                configuration.getLoadProfileEstimatorCache().optionallyProvideFor(configKey).orElse(null);
        if (cachedEstimator != null) return cachedEstimator.copy();

        final Optional<String> optSpecification = configuration.getOptionalStringProperty(configKey);
        if (optSpecification.isPresent()) {
            final NestableLoadProfileEstimator estimator =
                    LoadProfileEstimators.createFromSpecification(configKey, optSpecification.get());
            configuration.getLoadProfileEstimatorCache().set(configKey, estimator.copy());
            return estimator;
        } else {
            logger.warn("Could not find an estimator specification associated with '{}'.", configuration);
            return null;
        }
    }

    /**
     * Creates a new instance from a specification {@link String}. Valid specifications are as follows:
     * <pre>
     *     {"type":&lt;*juel*, org.qcri.rheem.core.util.mathex.mathex&gt;,
     *      "cpu":&lt;mathematical expression&gt;,
     *      "ram":&lt;mathematical expression&gt;,
     *      "disk":&lt;mathematical expression&gt;,
     *      "network":&lt;mathematical expression&gt;,
     *      "import":&lt;["optional", "operator", "properties"]&gt;,
     *      "in":&lt;#inputs&gt;,
     *      "out":&lt;#outputs&gt;,
     *      "p":&lt;correctness probability&gt;,
     *      "overhead":&lt;overhead in milliseconds&gt;,
     *      "ru":&lt;resource utilization mathematical expression&gt;
     *      }
     * </pre>
     * The JUEL expressions accept as parameters {@code in0}, {@code in1} a.s.o. for the input cardinalities and
     * {@code out0}, {@code out1} a.s.o. for the output cardinalities.
     *
     * @param configKey     the {@link Configuration} from that the {@code spec} was retrieved or else {@code null}
     * @param specification a specification that adheres to above format
     * @return the new instance
     */
    public static NestableLoadProfileEstimator createFromSpecification(String configKey, String specification) {
        try {
            final JSONObject spec = new JSONObject(specification);
            if (!spec.has("type") || "juel".equalsIgnoreCase(spec.getString("type"))) {
                return createFromJuelSpecification(configKey, spec);
            } else if ("mathex".equalsIgnoreCase(spec.getString("type"))) {
                return createFromMathExSpecification(configKey, spec);
            } else {
                throw new RheemException(String.format("Unknown specification type: %s", spec.get("type")));
            }
        } catch (Exception e) {
            throw new RheemException(String.format("Could not initialize from specification \"%s\".", specification), e);
        }
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
     * @param configKey the {@link Configuration} from that the {@code spec} was retrieved or else {@code null}
     * @param spec      a specification that adheres to above format
     * @return the new instance
     */
    public static NestableLoadProfileEstimator createFromJuelSpecification(String configKey, JSONObject spec) {
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
                overhead,
                configKey
        );
    }

    /**
     * Creates a new instance from a specification {@link String}. Valid specifications are as follows:
     * <pre>
     *     {"cpu":&lt;mathematical expression&gt;,
     *      "ram":&lt;mathematical expression&gt;,
     *      "disk":&lt;mathematical expression&gt;,
     *      "network":&lt;mathematical expression&gt;,
     *      "import":&lt;["optional", "operator", "properties"]&gt;,
     *      "in":&lt;#inputs&gt;,
     *      "out":&lt;#outputs&gt;,
     *      "p":&lt;correctness probability&gt;,
     *      "overhead":&lt;overhead in milliseconds&gt;,
     *      "ru":&lt;resource utilization mathematical expression&gt;
     *      }
     * </pre>
     * The JUEL expressions accept as parameters {@code in0}, {@code in1} a.s.o. for the input cardinalities and
     * {@code out0}, {@code out1} a.s.o. for the output cardinalities.
     *
     * @param configKey the {@link Configuration} from that the {@code spec} was retrieved or else {@code null}
     * @param spec      a specification that adheres to above format
     * @return the new instance
     */
    public static NestableLoadProfileEstimator createFromMathExSpecification(String configKey, JSONObject spec) {
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
                compile(spec.getString("cpu"))
        );
        LoadEstimator ramEstimator = new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                compile(spec.getString("ram"))
        );
        LoadEstimator diskEstimator = !spec.has("disk") ? null : new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                compile(spec.getString("disk"))
        );
        LoadEstimator networkEstimator = !spec.has("network") ? null : new DefaultLoadEstimator(
                numInputs,
                numOutputs,
                correctnessProb,
                CardinalityEstimate.EMPTY_ESTIMATE,
                compile(spec.getString("network"))
        );

        long overhead = spec.has("overhead") ? spec.getLong("overhead") : 0L;
        ToDoubleBiFunction<long[], long[]> resourceUtilizationEstimator = spec.has("ru") ?
                compileResourceUsage(spec.getString("ru")) :
                DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR;
        return new NestableLoadProfileEstimator(
                cpuEstimator,
                ramEstimator,
                diskEstimator,
                networkEstimator,
                resourceUtilizationEstimator,
                overhead,
                configKey
        );

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
        return (estimationContext, inCards, outCards) -> applyJuelFunction(juelFunction, estimationContext, inCards, outCards, additionalProperties);
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
                                           EstimationContext estimationContext,
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
            parameters.put(property, estimationContext.getDoubleProperty(property, 0d));
        }
        return juelFunction.apply(parameters, true);
    }

    /**
     * Parses a mathematical expression and provides it as a {@link SinglePointEstimationFunction}.
     *
     * @param expression a mathematical expression
     * @return the {@link SinglePointEstimationFunction}
     */
    private static SinglePointEstimationFunction compile(String expression) {
        final Expression expr = ExpressionBuilder.parse(expression).specify(baseContext);
        return (context, inCards, outCards) -> {
            Context mathContext = createMathContext(context, inCards, outCards);
            return Math.round(expr.evaluate(mathContext));
        };
    }

    /**
     * Parses a mathematical expression and provides it as a {@link ToDoubleFunction}.
     *
     * @param expression a mathematical expression
     * @return a {@link ToLongBiFunction} wrapping the expression
     */
    private static ToDoubleBiFunction<long[], long[]> compileResourceUsage(String expression) {
        final Expression expr = ExpressionBuilder.parse(expression).specify(baseContext);
        return (inCards, outCards) -> {
            Context mathContext = createMathContext(null, inCards, outCards);
            return expr.evaluate(mathContext);
        };
    }


    /**
     * Create a mathematical {@link Context} from the parameters.
     *
     * @param context             provides miscellaneous variables
     * @param inputCardinalities  provides input {@link CardinalityEstimate}s (`in***`)
     * @param outputCardinalities provides output {@link CardinalityEstimate}s (`out***`)
     * @return the {@link Context}
     */
    private static Context createMathContext(final EstimationContext context,
                                             final long[] inputCardinalities,
                                             final long[] outputCardinalities) {
        return new Context() {
            @Override
            public double getVariable(String variableName) throws EvaluationException {
                // Serve "in999" and "out999" variables directly from the cardinality arrays.
                if (variableName.startsWith("in") && variableName.length() > 2) {
                    int accu = 0;
                    int i;
                    for (i = 2; i < variableName.length(); i++) {
                        char c = variableName.charAt(i);
                        if (!Character.isDigit(c)) break;
                        accu = 10 * accu + (c - '0');
                    }
                    if (i == variableName.length()) return inputCardinalities[accu];
                } else if (variableName.startsWith("out") && variableName.length() > 3) {
                    int accu = 0;
                    int i;
                    for (i = 3; i < variableName.length(); i++) {
                        char c = variableName.charAt(i);
                        if (!Character.isDigit(c)) break;
                        accu = 10 * accu + (c - '0');
                    }
                    if (i == variableName.length()) return outputCardinalities[accu];
                }

                // Otherwise, ask the context for the property.
                return context.getDoubleProperty(variableName, Double.NaN);
            }

            @Override
            public ToDoubleFunction<double[]> getFunction(String functionName) throws EvaluationException {
                throw new EvaluationException("This context does not provide any functions.");
            }
        };
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
     * @param mainEstimatorOpt   an optional {@link LoadProfileEstimator}; should be a {@link NestableLoadProfileEstimator}
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
        mainEstimator.nest(subestimator);

    }


    private static final ToDoubleBiFunction<long[], long[]> DEFAULT_RESOURCE_UTILIZATION_ESTIMATOR = (in, out) -> 1d;

}
