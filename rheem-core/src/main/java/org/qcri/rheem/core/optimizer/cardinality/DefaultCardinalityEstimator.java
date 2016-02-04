package org.qcri.rheem.core.optimizer.cardinality;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.Arrays;
import java.util.Map;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;

/**
 * Default implementation of the {@link CardinalityEstimator}. Generalizes a single-point estimation function.
 */
public class DefaultCardinalityEstimator extends CardinalityEstimator.WithCache {

    private final double certaintyProb;

    private final int numInputs;

    private final ToLongBiFunction<long[], RheemContext> singlePointEstimator;

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       ToLongFunction<long[]> singlePointEstimator,
                                       OutputSlot<?> outputSlot,
                                       Map<OutputSlot<?>, CardinalityEstimate> cache) {
        this(certaintyProb,
                numInputs,
                (inputCards, rheemContext) -> singlePointEstimator.applyAsLong(inputCards),
                outputSlot,
                cache);
    }

    public DefaultCardinalityEstimator(double certaintyProb,
                                       int numInputs,
                                       ToLongBiFunction<long[], RheemContext> singlePointEstimator,
                                       OutputSlot<?> outputSlot,
                                       Map<OutputSlot<?>, CardinalityEstimate> cache) {
        super(outputSlot, cache);
        this.certaintyProb = certaintyProb;
        this.numInputs = numInputs;
        this.singlePointEstimator = singlePointEstimator;
    }


    @Override
    public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
        Validate.isTrue(inputEstimates.length == this.numInputs, "Received %d input estimates, require %d.",
                inputEstimates.length, this.numInputs);

        if (this.numInputs == 0) {
            final long estimate = this.singlePointEstimator.applyAsLong(new long[0], rheemContext);
            return new CardinalityEstimate(estimate, estimate, this.certaintyProb);
        }

        long[] lowerAndUpperInputEstimates = extractEstimateValues(inputEstimates);

        long lowerEstimate = -1, upperEstimate = -1;
        long[] currentInputEstimates = new long[this.numInputs];
        final int maximumBitMaskValue = 1 << this.numInputs;
        for (long positionBitmask = 0; positionBitmask < maximumBitMaskValue; positionBitmask++) {
            for (int pos = 0; pos < this.numInputs; pos++) {
                int bit = (int) ((positionBitmask >>> pos) & 0x1);
                currentInputEstimates[pos] = lowerAndUpperInputEstimates[(pos << 1) + bit];
            }
            long currentEstimate = Math.max(this.singlePointEstimator.applyAsLong(currentInputEstimates, rheemContext), 0);
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
