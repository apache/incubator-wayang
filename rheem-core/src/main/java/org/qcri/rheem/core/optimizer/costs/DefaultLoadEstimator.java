package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.Arrays;
import java.util.function.ToLongBiFunction;
import java.util.stream.LongStream;

/**
 * Implementation of {@link LoadEstimator} that uses a single-point cost function.
 */
public class DefaultLoadEstimator extends LoadEstimator {

    public static final int UNSPECIFIED_NUM_SLOTS = -1;

    private final double correctnessProbablity;

    private final int numInputs, numOutputs;

    private final ToLongBiFunction<long[], long[]> singlePointEstimator;

    public DefaultLoadEstimator(int numInputs,
                                int numOutputs,
                                double correctnessProbablity,
                                ToLongBiFunction<long[], long[]> singlePointFunction) {
        this(numInputs, numOutputs, correctnessProbablity, null, singlePointFunction);
    }

    public DefaultLoadEstimator(int numInputs,
                                int numOutputs,
                                double correctnessProbablity,
                                CardinalityEstimate nullCardinalityReplacement,
                                ToLongBiFunction<long[], long[]> singlePointFunction) {

        super(nullCardinalityReplacement);
        this.numInputs = numInputs;
        this.numOutputs = numOutputs;
        this.correctnessProbablity = correctnessProbablity;
        this.singlePointEstimator = singlePointFunction;
    }

    /**
     * Create a fallback {@link LoadEstimator} that accounts a given load for each input and output element. Missing
     * {@link CardinalityEstimate}s are interpreted as a cardinality of {@code 0}.
     */
    public static LoadEstimator createIOLinearEstimator(ExecutionOperator operator, long loadPerCardinalityUnit) {
        return new DefaultLoadEstimator(operator.getNumInputs(),
                operator.getNumOutputs(),
                0.01,
                CardinalityEstimate.EMPTY_ESTIMATE,
                (inputCards, outputCards) ->
                        loadPerCardinalityUnit * LongStream.concat(
                                Arrays.stream(inputCards),
                                Arrays.stream(outputCards)
                        ).sum()
        );
    }

    /**
     * Create a fallback {@link LoadEstimator} that accounts a given load for each input and output element.
     */
    public static LoadEstimator createIOLinearEstimator(long loadPerCardinalityUnit) {
        return new DefaultLoadEstimator(UNSPECIFIED_NUM_SLOTS, UNSPECIFIED_NUM_SLOTS,
                0.01,
                (inputCards, outputCards) ->
                        loadPerCardinalityUnit * LongStream.concat(
                                Arrays.stream(inputCards),
                                Arrays.stream(outputCards)
                        ).sum()
        );
    }

    @Override
    public LoadEstimate calculate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        Validate.isTrue(inputEstimates.length == this.numInputs || this.numInputs == UNSPECIFIED_NUM_SLOTS,
                "Received %d input estimates, require %d.", inputEstimates.length, this.numInputs);
        Validate.isTrue(outputEstimates.length == this.numOutputs || this.numOutputs == UNSPECIFIED_NUM_SLOTS,
                "Received %d output estimates, require %d.", outputEstimates.length, this.numOutputs);

        long[][] inputEstimateCombinations = this.enumerateCombinations(inputEstimates);
        long[][] outputEstimateCombinations = this.enumerateCombinations(outputEstimates);

        long lowerEstimate = -1, upperEstimate = -1;
        for (int inputEstimateId = 0; inputEstimateId < inputEstimateCombinations.length; inputEstimateId++) {
            for (int outputEstimateId = 0; outputEstimateId < outputEstimateCombinations.length; outputEstimateId++) {
                long estimate = Math.max(this.singlePointEstimator.applyAsLong(
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

    private long[][] enumerateCombinations(CardinalityEstimate[] cardinalityEstimates) {
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

}
