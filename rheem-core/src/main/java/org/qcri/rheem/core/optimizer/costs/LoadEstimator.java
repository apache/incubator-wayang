package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.Arrays;

/**
 * Describes the resource utilization of something executable, such as an {@link Operator} or a {@link FunctionDescriptor}.
 *
 * @param <Executable> the executable
 */
public abstract class LoadEstimator<Executable> {

    public static final int UNSPECIFIED_NUM_SLOTS = -1;

    /**
     * Functional interface for lambda expressions to express single-point load estimation functions.
     *
     * @param <T>
     */
    @FunctionalInterface
    public interface EstimationFunction<T> {

        /**
         * Estimate the load for the given artifact, input, and output estimates.
         *
         * @param artifact        the artifact
         * @param inputEstimates  the input cardinality estimates
         * @param outputEstimates the output cardinality estimates
         * @return the load estimate
         */
        long estimate(T artifact, long[] inputEstimates, long[] outputEstimates);

    }

    /**
     * Should be used to replace {@code null} {@link CardinalityEstimate}s.
     */
    protected final CardinalityEstimate nullCardinalityReplacement;

    /**
     * Create a new instance.
     *
     * @param nullCardinalityReplacement if an input {@link CardinalityEstimate} is {@code null}, it will be replaced
     *                                   with this default value
     */
    protected LoadEstimator(CardinalityEstimate nullCardinalityReplacement) {
        this.nullCardinalityReplacement = nullCardinalityReplacement;
    }

    /**
     * Calculate the {@link LoadEstimate}.
     *
     * @param executable      for that the {@link LoadEstimate} should be calculated
     * @param inputEstimates  {@link CardinalityEstimate}s for input data quanta
     * @param outputEstimates {@link CardinalityEstimate}s for output data quanta
     * @return the {@link LoadEstimate}
     */
    public abstract LoadEstimate calculate(Executable executable, CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates);

    /**
     * Utility method to calculate the probability that all cardinality estimates are correct.
     *
     * @param inputEstimates  some {@link CardinalityEstimate}s
     * @param outputEstimates more {@link CardinalityEstimate}s
     * @return the joint probability of all {@link CardinalityEstimate}s being correct
     */
    protected double calculateJointProbability(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return this.calculateJointProbability(inputEstimates) * this.calculateJointProbability(outputEstimates);
    }

    /**
     * Utility method to calculate the probability that all cardinality estimates are correct.
     *
     * @param estimates some {@link CardinalityEstimate}s
     * @return the joint probability of all {@link CardinalityEstimate}s being correct
     */
    private double calculateJointProbability(CardinalityEstimate[] estimates) {
        return Arrays.stream(estimates)
                .map(this::replaceNullCardinality)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .reduce(1d, (a, b) -> a * b);
    }

    /**
     * Create all possible combination of lower and upper estimates of the given {@link CardinalityEstimate}s,
     * thereby replacing {@code null} values according to {@link #replaceNullCardinality(CardinalityEstimate)}.
     *
     * @param cardinalityEstimates the {@link CardinalityEstimate}s
     * @return the enumerated combinations as two-dimensional {@code long[][]}
     */
    protected long[][] enumerateCombinations(CardinalityEstimate[] cardinalityEstimates) {
        if (cardinalityEstimates.length == 0) {
            return new long[1][0];
        }

        int numCombinations = 1 << cardinalityEstimates.length;
        long[][] combinations = new long[numCombinations][cardinalityEstimates.length];
        for (int combinationIdentifier = 0; combinationIdentifier < numCombinations; combinationIdentifier++) {
            for (int pos = 0; pos < cardinalityEstimates.length; pos++) {
                int bit = (combinationIdentifier >>> pos) & 0x1;
                final CardinalityEstimate cardinalityEstimate = this.replaceNullCardinality(cardinalityEstimates[pos]);
                combinations[combinationIdentifier][pos] = bit == 0 ?
                        cardinalityEstimate.getLowerEstimate() :
                        cardinalityEstimate.getUpperEstimate();
            }
        }

        return combinations;
    }

    /**
     * If the given {@code cardinalityEstimate} is {@code null} then return {@link #nullCardinalityReplacement},
     * otherwise {@code cardinalityEstimate}.
     */
    protected final CardinalityEstimate replaceNullCardinality(CardinalityEstimate cardinalityEstimate) {
        return cardinalityEstimate == null ? this.nullCardinalityReplacement : cardinalityEstimate;
    }

    /**
     * Create a fallback instance. See {@link DefaultLoadEstimator}.
     *
     * @param numInputs  the number of input slots of the estimation subject
     * @param numOutputs the number of output slots of the estimation subject
     * @return the fallback {@link LoadEstimator}
     */
    public static <T> DefaultLoadEstimator<T> createFallback(int numInputs, int numOutputs) {
        return new DefaultLoadEstimator<>(numInputs, numOutputs, 0.1d,
                (inputCards, outputCards) -> Arrays.stream(inputCards).sum() + Arrays.stream(outputCards).sum());
    }

}
