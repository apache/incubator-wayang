package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.Arrays;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.LongStream;

/**
 * Implementation of {@link LoadEstimator} that uses a single-point cost function.
 */
public class DefaultLoadEstimator extends LoadEstimator {

    private final double correctnessProbability;

    private final int numInputs, numOutputs;

    private final SinglePointEstimationFunction singlePointEstimator;

    public DefaultLoadEstimator(int numInputs,
                                int numOutputs,
                                double correctnessProbability,
                                ToLongBiFunction<long[], long[]> singlePointFunction) {
        this(numInputs, numOutputs, correctnessProbability, null, singlePointFunction);
    }

    public DefaultLoadEstimator(int numInputs,
                                int numOutputs,
                                double correctnessProbability,
                                CardinalityEstimate nullCardinalityReplacement,
                                ToLongBiFunction<long[], long[]> singlePointFunction) {
        this(
                numInputs, numOutputs, correctnessProbability, nullCardinalityReplacement,
                (context, inputEstimates, outputEstimates) -> singlePointFunction.applyAsLong(inputEstimates, outputEstimates)
        );
    }

    public DefaultLoadEstimator(int numInputs,
                                int numOutputs,
                                double correctnessProbability,
                                CardinalityEstimate nullCardinalityReplacement,
                                SinglePointEstimationFunction singlePointFunction) {
        super(nullCardinalityReplacement);
        this.numInputs = numInputs;
        this.numOutputs = numOutputs;
        this.correctnessProbability = correctnessProbability;
        this.singlePointEstimator = singlePointFunction;
    }

    /**
     * Utility to create new instances: Rounds the results of a given estimation function.
     */
    @SuppressWarnings("unused")
    public static ToLongBiFunction<long[], long[]> rounded(ToDoubleBiFunction<long[], long[]> f) {
        return (inputCards, outputCards) -> Math.round(f.applyAsDouble(inputCards, outputCards));
    }

    /**
     * Create a fallback {@link LoadEstimator} that accounts a given load for each input and output element.
     *
     * @param loadPerCardinalityUnit expected load units per input and output data quantum
     * @param confidence             confidence in the new instance
     */
    public static LoadEstimator createIOLinearEstimator(long loadPerCardinalityUnit, double confidence) {
        return createIOLinearEstimator(null, loadPerCardinalityUnit, confidence);
    }

    /**
     * Create a {@link LoadEstimator} that accounts a given load for each input and output element. Missing
     * {@link CardinalityEstimate}s are interpreted as a cardinality of {@code 0}.
     *
     * @param operator               an {@link ExecutionOperator} being addressed by the new instance
     * @param loadPerCardinalityUnit expected load units per input and output data quantum
     * @param confidence             confidence in the new instance
     */
    public static LoadEstimator createIOLinearEstimator(ExecutionOperator operator,
                                                        long loadPerCardinalityUnit,
                                                        double confidence) {
        return createIOLinearEstimator(operator, loadPerCardinalityUnit, confidence, CardinalityEstimate.EMPTY_ESTIMATE);
    }

    /**
     * Create a {@link LoadEstimator} that accounts a given load for each input and output element. Missing
     * {@link CardinalityEstimate}s are interpreted as a cardinality of {@code 0}.
     *
     * @param operator                   an {@link ExecutionOperator} being addressed by the new instance
     * @param loadPerCardinalityUnit     expected load units per input and output data quantum
     * @param confidence                 confidence in the new instance
     * @param nullCardinalityReplacement replacement for {@code null}s as {@link CardinalityEstimate}s
     */
    public static LoadEstimator createIOLinearEstimator(ExecutionOperator operator,
                                                        long loadPerCardinalityUnit,
                                                        double confidence,
                                                        CardinalityEstimate nullCardinalityReplacement) {
        return new DefaultLoadEstimator(
                operator == null ? UNSPECIFIED_NUM_SLOTS : operator.getNumInputs(),
                operator == null ? UNSPECIFIED_NUM_SLOTS : operator.getNumOutputs(),
                confidence,
                nullCardinalityReplacement,
                (inputCards, outputCards) ->
                        loadPerCardinalityUnit * LongStream.concat(
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
                long estimate = Math.max(this.singlePointEstimator.estimate(
                        context,
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
                * this.correctnessProbability;
        return new LoadEstimate(lowerEstimate, upperEstimate, correctnessProbability);
    }


}
