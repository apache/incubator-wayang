package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.JsonSerializables;

/**
 * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class LoadEstimate extends ProbabilisticIntervalEstimate implements JsonSerializable {

    public LoadEstimate(long exactValue) {
        this(exactValue, exactValue, 1d);
    }

    public LoadEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

    /**
     * Multiplies the estimated load. The correctness probability is not altered.
     *
     * @param n scalar to multiply with
     * @return the product
     */
    public LoadEstimate times(int n) {
        return new LoadEstimate(this.getLowerEstimate() * n, this.getUpperEstimate() * n, this.getCorrectnessProbability());
    }

    /**
     * Adds a this and the given instance.
     *
     * @param that the other summand
     * @return a new instance representing the sum
     */
    public LoadEstimate plus(LoadEstimate that) {
        return new LoadEstimate(
                this.getLowerEstimate() + that.getLowerEstimate(),
                this.getUpperEstimate() + that.getUpperEstimate(),
                this.getCorrectnessProbability() * that.getCorrectnessProbability()
        );
    }

    /**
     * Adds a the and the given instances.
     *
     * @param estimate1 the one summand or {@code null}
     * @param estimate2 the other summand or {@code null}
     * @return a new instance representing the sum or {@code null} if both summands are {@code null}
     */
    public static LoadEstimate add(LoadEstimate estimate1, LoadEstimate estimate2) {
        return estimate1 == null ?
                estimate2 :
                (estimate2 == null ? estimate1 : estimate1.plus(estimate2));
    }

    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("lower", JsonSerializables.serialize(this.getLowerEstimate(), false));
        json.put("upper", JsonSerializables.serialize(this.getUpperEstimate(), false));
        json.put("prob", JsonSerializables.serialize(this.getCorrectnessProbability(), false));
        return json;
    }

    @SuppressWarnings("unused")
    public static LoadEstimate fromJson(JSONObject jsonObject) {
        return new LoadEstimate(
                jsonObject.getLong("lower"),
                jsonObject.getLong("upper"),
                jsonObject.getDouble("prob")
        );
    }
}
