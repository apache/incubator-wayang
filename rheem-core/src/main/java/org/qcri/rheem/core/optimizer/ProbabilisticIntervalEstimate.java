package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.util.Formats;

import java.util.Comparator;
import java.util.Objects;

/***
 * An estimate that is capable of expressing uncertainty.
 * The estimate addresses uncertainty in the estimation process by
 * expressing estimates as intervals
 * and assigning a probability of correctness (in [0, 1]).
 ***/
public class ProbabilisticIntervalEstimate {

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

    /**
     * When merging instances somehow, overriding instance should be chosen over the others.
     */
    private final boolean isOverride;

    public ProbabilisticIntervalEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        this(lowerEstimate, upperEstimate, correctnessProb, false);
    }

    public ProbabilisticIntervalEstimate(long lowerEstimate, long upperEstimate, double correctnessProb, boolean isOverride) {
        assert lowerEstimate <= upperEstimate : String.format("%d > %d, which is illegal.", lowerEstimate, upperEstimate);
        assert correctnessProb >= 0 && correctnessProb <= 1 : String.format("Illegal probability %f.", correctnessProb);

        this.correctnessProb = correctnessProb;
        this.lowerEstimate = lowerEstimate;
        this.upperEstimate = upperEstimate;
        this.isOverride = isOverride;
    }

    public long getLowerEstimate() {
        return this.lowerEstimate;
    }

    public long getUpperEstimate() {
        return this.upperEstimate;
    }

    public long getAverageEstimate() {
        return (this.getUpperEstimate() + this.getLowerEstimate()) / 2;
    }

    public long getGeometricMeanEstimate() {
        return Math.round(Math.pow((double) this.getLowerEstimate() * (double) this.getUpperEstimate(), 0.5));
    }

    public double getCorrectnessProbability() {
        return this.correctnessProb;
    }

    /**
     * Checks whether this instance is an exact estimate of the given value.
     *
     * @param exactEstimate the hypothesized exact estimation value
     * @return whether this instance is exactly {@code exactEstimate}
     */
    public boolean isExactly(long exactEstimate) {
        return this.isExact() && this.upperEstimate == exactEstimate;
    }

    /**
     * Checks whether this instance is an exact value, i.e., it is a confident single-point estimate.
     *
     * @return whether this instance is exact
     */
    public boolean isExact() {
        return this.correctnessProb == 1d && this.lowerEstimate == this.upperEstimate;
    }

    /**
     * Creates a {@link Comparator} based on the mean of instances.
     */
    public static <T extends ProbabilisticIntervalEstimate> Comparator<T> expectationValueComparator() {
        return (t1, t2) -> {
            if (t1.getCorrectnessProbability() == 0d) {
                if (t2.getCorrectnessProbability() != 0d) {
                    return 1;
                }
            } else if (t2.getCorrectnessProbability() == 0d) {
                return -1;
            }
            // NB: We do not assume a uniform distribution of the estimates within the instances.
            return Long.compare(t1.getGeometricMeanEstimate(), t2.getGeometricMeanEstimate());
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        ProbabilisticIntervalEstimate estimate = (ProbabilisticIntervalEstimate) o;
        return Double.compare(estimate.correctnessProb, this.correctnessProb) == 0 &&
                this.lowerEstimate == estimate.lowerEstimate &&
                this.upperEstimate == estimate.upperEstimate;
    }

    /**
     * Compares with this instance equals with {@code that} instance within given delta bounds.
     */
    public boolean equalsWithinDelta(ProbabilisticIntervalEstimate that,
                                     double probDelta,
                                     long lowerEstimateDelta,
                                     long upperEstimateDelta) {
        return Math.abs(that.correctnessProb - this.correctnessProb) <= probDelta &&
                Math.abs(this.lowerEstimate - that.lowerEstimate) <= lowerEstimateDelta &&
                Math.abs(this.upperEstimate - that.upperEstimate) <= upperEstimateDelta;
    }

    public boolean isOverride() {
        return this.isOverride;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.correctnessProb, this.lowerEstimate, this.upperEstimate);
    }

    @Override
    public String toString() {
        return String.format("%s[%s..%s, %s]", this.getClass().getSimpleName(),
                Formats.formatLarge(this.lowerEstimate), Formats.formatLarge(this.upperEstimate), Formats.formatPercentage(this.correctnessProb)
        );
    }
}
