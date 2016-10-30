package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;

import java.util.Arrays;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;

/**
 * Default implementation of the {@link CardinalityEstimator}. Generalizes a single-point estimation function.
 */
public class DefaultCardinalityEstimator implements CardinalityEstimator {

    private final double certaintyProb;

    private final int numInputs;

    private final ToLongBiFunction<long[], Configuration> singlePointEstimator;

    /**
     * If {@code true}, receiving more than {@link #numInputs} is also fine.
     */
    private final boolean isAllowMoreInputs;

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       boolean isAllowMoreInputs,
                                       ToLongFunction<long[]> singlePointEstimator) {
        this(certaintyProb,
                numInputs,
                isAllowMoreInputs,
                (inputCards, rheemContext) -> singlePointEstimator.applyAsLong(inputCards));
    }

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       boolean isAllowMoreInputs,
                                       ToLongBiFunction<long[], Configuration> singlePointEstimator) {
        this.certaintyProb = certaintyProb;
        this.numInputs = numInputs;
        this.singlePointEstimator = singlePointEstimator;
        this.isAllowMoreInputs = isAllowMoreInputs;
    }


    @Override
    public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
        assert inputEstimates.length == this.numInputs
                || (this.isAllowMoreInputs && inputEstimates.length > this.numInputs) :
                String.format("Received %d input estimates, require %d%s.",
                        inputEstimates.length, this.numInputs, this.isAllowMoreInputs ? "+" : "");

        if (this.numInputs == 0) {
            final long estimate = this.singlePointEstimator.applyAsLong(new long[0], optimizationContext.getConfiguration());
            return new CardinalityEstimate(estimate, estimate, this.certaintyProb);
        }

        long[] lowerAndUpperInputEstimates = this.extractEstimateValues(inputEstimates);

        long lowerEstimate = -1, upperEstimate = -1;
        long[] currentInputEstimates = new long[this.numInputs];
        final int maximumBitMaskValue = 1 << this.numInputs;
        for (long positionBitmask = 0; positionBitmask < maximumBitMaskValue; positionBitmask++) {
            for (int pos = 0; pos < this.numInputs; pos++) {
                int bit = (int) ((positionBitmask >>> pos) & 0x1);
                currentInputEstimates[pos] = lowerAndUpperInputEstimates[(pos << 1) + bit];
            }
            long currentEstimate = Math.max(
                    this.singlePointEstimator.applyAsLong(currentInputEstimates, optimizationContext.getConfiguration()),
                    0
            );
            if (lowerEstimate == -1 || currentEstimate < lowerEstimate) {
                lowerEstimate = currentEstimate;
            }
            if (upperEstimate == -1 || currentEstimate > upperEstimate) {
                upperEstimate = currentEstimate;
            }
        }

        double correctnessProb = this.certaintyProb * Arrays.stream(inputEstimates)
                .mapToDouble(CardinalityEstimate::getCorrectnessProbability)
                .min()
                .orElseThrow(IllegalStateException::new);
        return new CardinalityEstimate(lowerEstimate, upperEstimate, correctnessProb);
    }

    private long[] extractEstimateValues(CardinalityEstimate[] inputEstimates) {
        long[] lowerAndUpperEstimates = new long[this.numInputs * 2];
        for (int i = 0; i < this.numInputs; i++) {
            final CardinalityEstimate inputEstimate = inputEstimates[i];
            lowerAndUpperEstimates[i << 1] = inputEstimate.getLowerEstimate();
            lowerAndUpperEstimates[(i << 1) + 1] = inputEstimate.getUpperEstimate();
        }
        return lowerAndUpperEstimates;
    }
}
