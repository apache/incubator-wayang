package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Arrays;

/**
 * Describes the resource utilization of something executable.
 */
public abstract class LoadEstimator {

    /**
     * Should be used to replace {@code null} {@link CardinalityEstimate}s.
     */
    protected final CardinalityEstimate nullCardinalityReplacement;

    protected LoadEstimator(CardinalityEstimate nullCardinalityReplacement) {
        this.nullCardinalityReplacement = nullCardinalityReplacement;
    }

    public abstract LoadEstimate calculate(
            CardinalityEstimate[] inputEstimates,
            CardinalityEstimate[] outputEstimates);

    protected double calculateJointProbability(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return this.calculateJointProbability(inputEstimates) * this.calculateJointProbability(outputEstimates);
    }

    private double calculateJointProbability(CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates)
                .map(this::replaceNullCardinality)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .reduce(1d, (a, b) -> a * b);
    }

    /**
     * If the given {@code cardinalityEstimate} is {@code null} then return {@link #nullCardinalityReplacement},
     * otherwise {@code cardinalityEstimate}.
     */
    protected final CardinalityEstimate replaceNullCardinality(CardinalityEstimate cardinalityEstimate) {
        return cardinalityEstimate == null ? this.nullCardinalityReplacement : cardinalityEstimate;
    }

    public static LoadEstimator createFallback(int numInputs, int numOutputs) {
        return new DefaultLoadEstimator(numInputs, numOutputs, 0.1d,
                (inputCards, outputCards) -> Arrays.stream(inputCards).sum() + Arrays.stream(outputCards).sum());
    }

}
