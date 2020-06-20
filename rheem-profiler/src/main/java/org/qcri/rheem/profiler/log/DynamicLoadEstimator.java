package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.util.mathex.Context;
import org.qcri.rheem.core.util.mathex.DefaultContext;
import org.qcri.rheem.core.util.mathex.Expression;
import org.qcri.rheem.core.util.mathex.ExpressionBuilder;
import org.qcri.rheem.core.util.mathex.exceptions.EvaluationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * Adjustable {@link LoadProfileEstimator} implementation.
 */
public class DynamicLoadEstimator extends LoadEstimator {

    /**
     * Instance that always estimates a load of {@code 0}.
     */
    public static DynamicLoadEstimator zeroLoad = new DynamicLoadEstimator(
            (individual, inputCardinalities, outputCardinalities) -> 0d,
            individual -> "0",
            Collections.emptySet()
    );

    /**
     * Function to estimate the load for given {@link Individual}.
     */
    private final SinglePointEstimator singlePointEstimator;

    /**
     * {@link Variable}s used in the {@link #singlePointEstimator}.
     */
    private final Collection<Variable> employedVariables;

    /**
     * Maps an {@link Individual} to a MathEx specifation {@link String}.
     */
    private final Function<Individual, String> specificationBuilder;

    @FunctionalInterface
    public interface SinglePointEstimator {

        double estimate(DynamicEstimationContext context, long[] inputCardinalities, long[] outputCardinalities);

    }

    /**
     * Parses a mathematical expression and provides it as a {@link LoadEstimator.SinglePointEstimationFunction}.
     *
     * @param templateKey       of the {@code expression}
     * @param resource          that is being estimated
     * @param expression        a mathematical expression
     * @param optimizationSpace in which new {@link Variable}s should be created
     * @return the {@link LoadEstimator.SinglePointEstimationFunction}
     */
    public static DynamicLoadEstimator createFor(String templateKey,
                                                 String resource,
                                                 String expression,
                                                 OptimizationSpace optimizationSpace) {
        // Replace the question marks with variable names.
        Collection<Variable> variables = new LinkedList<>();
        StringBuilder sb = new StringBuilder(2 * expression.length());
        for (int i = 0; i < expression.length(); i++) {
            final char c = expression.charAt(i);
            if (c == '?') {
                String variableId = templateKey + "." + resource + "[" + variables.size() + "]";
                final Variable variable = optimizationSpace.getOrCreateVariable(variableId);
                variables.add(variable);
                sb.append("_var").append(variable.getIndex());
            } else {
                sb.append(c);
            }
        }

        final Expression expr = ExpressionBuilder.parse(sb.toString()).specify(LoadProfileEstimators.baseContext);

        SinglePointEstimator singlePointEstimator =
                (individual, inCards, outCards) -> {
                    Context mathContext = createMathContext(individual, inCards, outCards);
                    return Math.round(expr.evaluate(mathContext));
                };
        final Function<Individual, String> specificationBuilder = individual -> {
            DefaultContext context = new DefaultContext();
            for (Variable variable : variables) {
                final double value = variable.getValue(individual);
                final String name = "_var" + variable.getIndex();
                context.setVariable(name, value);
            }
            return expr.specify(context).toString();
        };

        return new DynamicLoadEstimator(singlePointEstimator, specificationBuilder, variables);
    }

    /**
     * Create a mathematical {@link Context} from the parameters.
     *
     * @param context             provides miscellaneous variables and the {@link Individual}
     * @param inputCardinalities  provides input {@link CardinalityEstimate}s (`in***`)
     * @param outputCardinalities provides output {@link CardinalityEstimate}s (`out***`)
     * @return the {@link Context}
     */
    private static Context createMathContext(final DynamicEstimationContext context,
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
                } else if (variableName.startsWith("_var") && variableName.length() > 4) {
                    int accu = 0;
                    int i;
                    for (i = 4; i < variableName.length(); i++) {
                        char c = variableName.charAt(i);
                        if (!Character.isDigit(c)) break;
                        accu = 10 * accu + (c - '0');
                    }
                    if (i == variableName.length()) return context.getIndividual().getGenome()[accu];
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
     * Creates a new instance.
     *
     * @param singlePointEstimator the {@link SinglePointEstimator} to use
     * @param specificationBuilder creates a MathEx specification for the new instance
     *                             with the parameters from an {@link Individual}
     * @param employedVariables    the {@link Variable}s appearing in the {@code singlePointEstimator}
     */
    public DynamicLoadEstimator(SinglePointEstimator singlePointEstimator,
                                Function<Individual, String> specificationBuilder,
                                Variable... employedVariables) {
        this(singlePointEstimator, specificationBuilder, Arrays.asList(employedVariables));
    }

    /**
     * Creates a new instance.
     *
     * @param singlePointEstimator the {@link SinglePointEstimator} to use
     * @param employedVariables    the {@link Variable}s appearing in the {@code singlePointEstimator}
     */
    public DynamicLoadEstimator(SinglePointEstimator singlePointEstimator,
                                Function<Individual, String> specificationBuilder,
                                Collection<Variable> employedVariables) {
        super(CardinalityEstimate.EMPTY_ESTIMATE);
        this.singlePointEstimator = singlePointEstimator;
        this.specificationBuilder = specificationBuilder;
        this.employedVariables = employedVariables;
    }

    @Override
    public LoadEstimate calculate(EstimationContext context) {
        if (!(context instanceof DynamicEstimationContext)) {
            throw new IllegalArgumentException("Invalid estimation context.");
        }
        final DynamicEstimationContext dynamicContext = (DynamicEstimationContext) context;
        final CardinalityEstimate[] inputEstimates = context.getInputCardinalities();
        final CardinalityEstimate[] outputEstimates = context.getOutputCardinalities();
        long[] inputCardinalities = new long[inputEstimates.length];
        long[] outputCardinalities = new long[outputEstimates.length];
        for (int i = 0; i < inputEstimates.length; i++) {
            inputCardinalities[i] = this.replaceNullCardinality(inputEstimates[i]).getLowerEstimate();
        }
        for (int i = 0; i < outputEstimates.length; i++) {
            outputCardinalities[i] = this.replaceNullCardinality(outputEstimates[i]).getLowerEstimate();
        }
        double lowerEstimate = this.singlePointEstimator.estimate(
                dynamicContext, inputCardinalities, outputCardinalities
        );
        for (int i = 0; i < inputEstimates.length; i++) {
            inputCardinalities[i] = this.replaceNullCardinality(inputEstimates[i]).getUpperEstimate();
        }
        for (int i = 0; i < outputEstimates.length; i++) {
            outputCardinalities[i] = this.replaceNullCardinality(outputEstimates[i]).getUpperEstimate();
        }
        double upperEstimate = this.singlePointEstimator.estimate(
                dynamicContext, inputCardinalities, outputCardinalities
        );
        return new LoadEstimate(
                Math.round(lowerEstimate),
                Math.round(upperEstimate),
                this.calculateJointProbability(inputEstimates, outputEstimates)
        );
    }

    /**
     * Creates a MathEx expression reflecting this instance under the configuration specified by an {@link Individual}.
     *
     * @param individual specifies values of the employed {@link Variable}s
     * @return the MathEx expression
     */
    public String toMathEx(Individual individual) {
        return this.specificationBuilder.apply(individual);
    }

    /**
     * Get the {@link Variable}s used in this instance.
     *
     * @return the {@link Variable}s
     */
    public Collection<Variable> getEmployedVariables() {
        return this.employedVariables;
    }

}
