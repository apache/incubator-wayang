package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Arrays;

/**
 * Describes the resource utilization of something executable.
 */
public abstract class LoadEstimator {

    public abstract LoadEstimate calculate(
            CardinalityEstimate[] inputEstimates,
            CardinalityEstimate[] outputEstimates);

    protected double calculateJointProbability(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return this.calculateJointProbability(inputEstimates) * this.calculateJointProbability(outputEstimates);
    }

    private double calculateJointProbability(CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .reduce(1d, (a, b) -> a * b);
    }

    public static LoadEstimator createFallback(int numInputs, int numOutputs) {
        return new DefaultLoadEstimator(numInputs, numOutputs, 0.1d,
                (inputCards, outputCards) -> Arrays.stream(inputCards).sum() + Arrays.stream(outputCards).sum());
    }

}
