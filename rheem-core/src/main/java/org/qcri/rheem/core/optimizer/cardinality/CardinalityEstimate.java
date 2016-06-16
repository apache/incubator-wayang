package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

/**
 * An estimate of cardinality within a {@link RheemPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class CardinalityEstimate extends ProbabilisticIntervalEstimate {

    public static final CardinalityEstimate EMPTY_ESTIMATE = new CardinalityEstimate(0, 0, 1d);

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb, boolean isOverride) {
        super(lowerEstimate, upperEstimate, correctnessProb, isOverride);
    }

    public CardinalityEstimate plus(CardinalityEstimate that) {
        return new CardinalityEstimate(
                addSafe(this.getLowerEstimate(), that.getLowerEstimate()),
                addSafe(this.getUpperEstimate(), that.getUpperEstimate()),
                Math.min(this.getCorrectnessProbability(), that.getCorrectnessProbability())
        );
    }

    /**
     * Avoids buffer overflows while adding two positive {@code long}s.
     */
    private static final long addSafe(long a, long b) {
        assert a >= 0 && b >= 0;
        long sum = a + b;
        if (sum < a || sum < b) sum = Long.MAX_VALUE;
        return sum;
    }

    /**
     * Divides the estimate values, not the probability.
     *
     * @param denominator by which this instance should be divided
     * @return the quotient
     */
    public CardinalityEstimate divideBy(double denominator) {
        return new CardinalityEstimate(
                (long) Math.ceil(this.getLowerEstimate() / denominator),
                (long) Math.ceil(this.getUpperEstimate() / denominator),
                this.getCorrectnessProbability()
        );
    }
}
