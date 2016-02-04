package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Arrays;

/**
 * Describes the resource utilization of something executable.
 */
public abstract class ResourceFunction {

    public abstract ResourceUsageEstimate calculate(
            CardinalityEstimate[] inputEstimates,
            CardinalityEstimate[] outputEstimates);

    protected double calculateJointProbability(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return calculateJointProbability(inputEstimates) * calculateJointProbability(outputEstimates);
    }

    private double calculateJointProbability(CardinalityEstimate[] inputEstimates) {
        return Arrays.stream(inputEstimates)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .reduce(1d, (a, b) -> a * b);
    }

    public static ResourceFunction createFallback(int numInputs, int numOutputs) {
        return new DefaultResourceFunction(numInputs, numOutputs, 0.1d,
                (inputCards, outputCards) -> Arrays.stream(inputCards).sum() + Arrays.stream(outputCards).sum());
    }

}
