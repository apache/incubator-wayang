package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Adjustable {@link LoadProfileEstimator} implementation.
 */
public class DynamicLoadEstimator extends LoadEstimator {

    /**
     * Instance that always estimates a load of {@code 0}.
     */
    public static DynamicLoadEstimator zeroLoad = new DynamicLoadEstimator(
            (individual, inputCardinalities, outputCardinalities) -> 0d,
            "0",
            Collections.emptySet()
    );

    /**
     * Template to produce a JUEL expression reflecting this instance.
     */
    private final String juelTemplate;

    /**
     * Function to estimate the load for given {@link Individual}.
     */
    private final SinglePointEstimator singlePointEstimator;

    /**
     * {@link Variable}s used in the {@link #singlePointEstimator}.
     */
    private final Collection<Variable> employedVariables;

    @FunctionalInterface
    public interface SinglePointEstimator {

        double estimate(Individual individual, long[] inputCardinalities, long[] outputCardinalities);

    }

    /**
     * Creates a new instance.
     *
     * @param singlePointEstimator the {@link SinglePointEstimator} to use
     * @param juelTemplate         template for creating a JUEL expression reflecting the {@code singlePointEstimator};
     *                             formatting arguments are the {@code employedVariables}
     * @param employedVariables    the {@link Variable}s appearing in the {@code singlePointEstimator}
     */
    public DynamicLoadEstimator(SinglePointEstimator singlePointEstimator,
                                String juelTemplate,
                                Variable... employedVariables) {
        this(singlePointEstimator, juelTemplate, Arrays.asList(employedVariables));
    }

    /**
     * Creates a new instance.
     *
     * @param singlePointEstimator the {@link SinglePointEstimator} to use
     * @param juelTemplate         template for creating a JUEL expression reflecting the {@code singlePointEstimator};
     *                             formatting arguments are the {@code employedVariables}
     * @param employedVariables    the {@link Variable}s appearing in the {@code singlePointEstimator}
     */
    public DynamicLoadEstimator(SinglePointEstimator singlePointEstimator,
                                String juelTemplate,
                                Collection<Variable> employedVariables) {
        super(CardinalityEstimate.EMPTY_ESTIMATE);
        this.singlePointEstimator = singlePointEstimator;
        this.juelTemplate = juelTemplate;
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
                dynamicContext.getIndividual(), inputCardinalities, outputCardinalities
        );
        for (int i = 0; i < inputEstimates.length; i++) {
            inputCardinalities[i] = this.replaceNullCardinality(inputEstimates[i]).getUpperEstimate();
        }
        for (int i = 0; i < outputEstimates.length; i++) {
            outputCardinalities[i] = this.replaceNullCardinality(outputEstimates[i]).getUpperEstimate();
        }
        double upperEstimate = this.singlePointEstimator.estimate(
                dynamicContext.getIndividual(), inputCardinalities, outputCardinalities
        );
        return new LoadEstimate(
                Math.round(lowerEstimate),
                Math.round(upperEstimate),
                this.calculateJointProbability(inputEstimates, outputEstimates)
        );
    }

    /**
     * Creates a JUEL expression reflecting this instance under the configuration specified by an {@link Individual}.
     *
     * @param individual specifies values of the employed {@link Variable}s
     * @return the JUEL expression
     */
    public String toJuel(Individual individual) {
        Object[] formatArgs = new Object[this.employedVariables.size()];
        int i = 0;
        for (Variable variable : employedVariables) {
            formatArgs[i++] = variable.getValue(individual);
        }
        return String.format(this.juelTemplate, formatArgs);

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
