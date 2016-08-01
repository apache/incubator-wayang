package org.qcri.rheem.core.optimizer.cardinality;

import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.JsonSerializable;

/**
 * An estimate of cardinality within a {@link RheemPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class CardinalityEstimate extends ProbabilisticIntervalEstimate implements JsonSerializable {

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

    /**
     * Parses the given {@link JSONObject} to create a new instance.
     *
     * @param json that should be parsed
     * @return the new instance
     */
    public static CardinalityEstimate fromJson(JSONObject json) {
        return new CardinalityEstimate(json.getLong("lowerBound"), json.getLong("upperBound"), json.getDouble("confidence"));
    }

    @Override
    public JSONObject toJson() {
        return this.toJson(new JSONObject());
    }

    /**
     * Serializes this instance to the given {@link JSONObject}.
     *
     * @param json to which this instance should be serialized
     * @return {@code json}
     */
    public JSONObject toJson(JSONObject json) {
        json.put("lowerBound", this.getLowerEstimate());
        json.put("upperBound", this.getUpperEstimate());
        json.put("confidence", this.getCorrectnessProbability());
        return json;
    }

    @Override
    public String toString() {
        return String.format(
                "(%,d..%,d, %s)",
                this.getLowerEstimate(),
                this.getUpperEstimate(),
                Formats.formatPercentage(this.getCorrectnessProbability())
        );
    }
}
