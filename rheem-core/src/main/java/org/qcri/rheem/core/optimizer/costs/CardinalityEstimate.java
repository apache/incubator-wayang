package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.PhysicalPlan;

/**
 * An estimate of cardinality within a {@link PhysicalPlan}.
 * <p>The estimate addresses uncertainty in the estimation process by
 * <ol><li>expressing estimates as intervals</li>
 * <li>and assigning a probability of correctness (in [0, 1]).</li></ol></p>
 */
public class CardinalityEstimate {

    /**
     * Probability of correctness between in the interval [0, 1]. This helps
     * Rheem in situations with many estimates to pick the best one.
     */
    private final double correctnessProb;

    /**
     * Lower and upper estimate. Not that this is not a bounding box, i.e., there is no guarantee that the finally
     * observed value will be within the estimated interval.
     */
    private final long lowerEstimate, upperEstimate;

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        Validate.isTrue(lowerEstimate <= upperEstimate);
        Validate.inclusiveBetween(0, 1, correctnessProb);

        this.correctnessProb = correctnessProb;
        this.lowerEstimate = lowerEstimate;
        this.upperEstimate = upperEstimate;
    }

    public long getLowerEstimate() {
        return lowerEstimate;
    }

    public long getUpperEstimate() {
        return upperEstimate;
    }

    public double getCorrectnessProbability() {
        return correctnessProb;
    }
}
