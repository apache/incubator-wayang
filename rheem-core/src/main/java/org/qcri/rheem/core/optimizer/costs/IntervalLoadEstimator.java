package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.Arrays;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.LongStream;

/**
 * Implementation of {@link LoadEstimator} that uses a interval-based cost function.
 */
public class IntervalLoadEstimator extends LoadEstimator {

    private final double correctnessProbablity;

    private final int numInputs, numOutputs;

    private final ToLongBiFunction<long[], long[]> lowerBoundEstimator, upperBoundEstimator;

    public IntervalLoadEstimator(int numInputs,
                                 int numOutputs,
                                 double correctnessProbability,
                                 ToLongBiFunction<long[], long[]> lowerBoundEstimator,
                                 ToLongBiFunction<long[], long[]> upperBoundEstimator) {
        this(numInputs, numOutputs, correctnessProbability, null, lowerBoundEstimator, upperBoundEstimator);
    }

    public IntervalLoadEstimator(int numInputs,
                                 int numOutputs,
                                 double correctnessProbablity,
                                 CardinalityEstimate nullCardinalityReplacement,
                                 ToLongBiFunction<long[], long[]> lowerBoundEstimator,
                                 ToLongBiFunction<long[], long[]> upperBoundEstimator) {

        super(nullCardinalityReplacement);
        this.numInputs = numInputs;
        this.numOutputs = numOutputs;
        this.correctnessProbablity = correctnessProbablity;
        this.lowerBoundEstimator = lowerBoundEstimator;
        this.upperBoundEstimator = upperBoundEstimator;
    }

    /**
     * Utility to create new instances: Rounds the results of a given estimation function.
     */
    @SuppressWarnings("unused")
    public static ToLongBiFunction<long[], long[]> rounded(ToDoubleBiFunction<long[], long[]> f) {
        return (inputCards, outputCards) -> Math.round(f.applyAsDouble(inputCards, outputCards));
    }

    /**
     * Create a {@link LoadEstimator} that accounts a given load for each input and output element.
     *
     * @param operator                    an {@link ExecutionOperator} being addressed by the new instance
     * @param lowerLoadPerCardinalityUnit lower expected load units per input and output data quantum
     * @param upperLoadPerCardinalityUnit upper expected load units per input and output data quantum
     * @param confidence                  confidence in the new instance
     * @param nullCardinalityReplacement  replacement for {@code null}s as {@link CardinalityEstimate}s
     */
    public static <T extends ExecutionOperator> LoadEstimator createIOLinearEstimator(
            T operator,
            long lowerLoadPerCardinalityUnit,
            long upperLoadPerCardinalityUnit,
            double confidence,
            CardinalityEstimate nullCardinalityReplacement) {
        return new IntervalLoadEstimator(
                operator == null ? UNSPECIFIED_NUM_SLOTS : operator.getNumInputs(),
                operator == null ? UNSPECIFIED_NUM_SLOTS : operator.getNumOutputs(),
                confidence,
                nullCardinalityReplacement,
                (inputCards, outputCards) ->
                        lowerLoadPerCardinalityUnit * LongStream.concat(
                                Arrays.stream(inputCards),
                                Arrays.stream(outputCards)
                        ).sum(),
                (inputCards, outputCards) ->
                        upperLoadPerCardinalityUnit * LongStream.concat(
                                Arrays.stream(inputCards),
                                Arrays.stream(outputCards)
                        ).sum()
        );
    }

    @Override
    public LoadEstimate calculate(EstimationContext context) {
        final CardinalityEstimate[] inputEstimates = context.getInputCardinalities();
        final CardinalityEstimate[] outputEstimates = context.getOutputCardinalities();
        Validate.isTrue(inputEstimates.length >= this.numInputs || this.numInputs == UNSPECIFIED_NUM_SLOTS,
                "Received %d input estimates, require %d.", inputEstimates.length, this.numInputs);
        Validate.isTrue(outputEstimates.length == this.numOutputs || this.numOutputs == UNSPECIFIED_NUM_SLOTS,
                "Received %d output estimates, require %d.", outputEstimates.length, this.numOutputs);

        long[][] inputEstimateCombinations = this.enumerateCombinations(inputEstimates);
        long[][] outputEstimateCombinations = this.enumerateCombinations(outputEstimates);

        long lowerEstimate = -1, upperEstimate = -1;
        for (int inputEstimateId = 0; inputEstimateId < inputEstimateCombinations.length; inputEstimateId++) {
            for (int outputEstimateId = 0; outputEstimateId < outputEstimateCombinations.length; outputEstimateId++) {
                long estimate = Math.max(this.lowerBoundEstimator.applyAsLong(
                        inputEstimateCombinations[inputEstimateId],
                        outputEstimateCombinations[outputEstimateId]
                ), 0);
                if (lowerEstimate == -1 || estimate < lowerEstimate) {
                    lowerEstimate = estimate;
                }
                if (upperEstimate == -1 || estimate > upperEstimate) {
                    upperEstimate = estimate;
                }
                estimate = Math.max(this.upperBoundEstimator.applyAsLong(
                        inputEstimateCombinations[inputEstimateId],
                        outputEstimateCombinations[outputEstimateId]
                ), 0);
                if (lowerEstimate == -1 || estimate < lowerEstimate) {
                    lowerEstimate = estimate;
                }
                if (upperEstimate == -1 || estimate > upperEstimate) {
                    upperEstimate = estimate;
                }
            }
        }

        double correctnessProbability = this.calculateJointProbability(inputEstimates, outputEstimates)
                * this.correctnessProbablity;
        return new LoadEstimate(lowerEstimate, upperEstimate, correctnessProbability);
    }


}
