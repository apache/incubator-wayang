package org.qcri.rheem.core.optimizer;

import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

/***
 * An value representation that is capable of expressing uncertainty.
 * It addresses uncertainty by expressing estimates as intervals and assigning a probability of correctness (in [0, 1]).
 */
public class ProbabilisticDoubleInterval {

    /**
     * Instance that basically represents the value {@code 0d}.
     */
    public static final ProbabilisticDoubleInterval zero = ProbabilisticDoubleInterval.ofExactly(0d);

    /**
     * Provides a {@link Comparator} for {@link ProbabilisticDoubleInterval}s.
     * For two {@link ProbabilisticDoubleInterval}s {@code t1} and {@code t2}, it works as follows:
     * <ol>
     * <li>If a either of the {@link ProbabilisticDoubleInterval}s has a correctness probability of 0, we consider it to be greater.</li>
     * <li>Otherwise, we compare the two {@link ProbabilisticDoubleInterval}s by their average estimation value.</li>
     * </ol>
     *
     * @return
     */
    public static Comparator<ProbabilisticDoubleInterval> expectationValueComparator() {
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

    /**
     * Provides a {@link Comparator} for {@link ProbabilisticDoubleInterval}s.
     * It provides a random, but consistent order.
     *
     * @return a new random {@link Comparator}
     */
    public static Comparator<ProbabilisticDoubleInterval> randomComparator() {
        final int salt = new Random().nextInt();
        return (t1, t2) -> Integer.compare(t1.hashCode() * salt + t1.hashCode(), t2.hashCode() * salt + t2.hashCode());
    }

    /**
     * Probability of correctness between in the interval [0, 1]. This helps
     * Rheem in situations with many estimates to pick the best one.
     */
    private final double correctnessProb;

    /**
     * Lower and upper estimate. Not that this is not a bounding box, i.e., there is no guarantee that the finally
     * observed value will be within the estimated interval.
     */
    private final double lowerEstimate, upperEstimate;

    /**
     * When merging instances somehow, overriding instance should be chosen over the others.
     */
    private final boolean isOverride;

    /**
     * Creates a new instance with a zero-width interval and a confidence of {@code 1}.
     *
     * @param value lower and upper value
     * @return the new instance
     */
    public static ProbabilisticDoubleInterval ofExactly(double value) {
        return new ProbabilisticDoubleInterval(value, value, 1d);
    }

    public ProbabilisticDoubleInterval(double lowerEstimate, double upperEstimate, double correctnessProb) {
        this(lowerEstimate, upperEstimate, correctnessProb, false);
    }

    public ProbabilisticDoubleInterval(double lowerEstimate, double upperEstimate, double correctnessProb, boolean isOverride) {
        assert lowerEstimate <= upperEstimate : String.format("%f > %f, which is illegal.", lowerEstimate, upperEstimate);
        assert correctnessProb >= 0 && correctnessProb <= 1 : String.format("Illegal probability %f.", correctnessProb);

        this.correctnessProb = correctnessProb;
        this.lowerEstimate = lowerEstimate;
        this.upperEstimate = upperEstimate;
        this.isOverride = isOverride;
    }

    public double getLowerEstimate() {
        return this.lowerEstimate;
    }

    public double getUpperEstimate() {
        return this.upperEstimate;
    }

    public double getAverageEstimate() {
        return (this.getUpperEstimate() + this.getLowerEstimate()) / 2;
    }

    public long getGeometricMeanEstimate() {
        return Math.round(Math.pow(this.getLowerEstimate() * this.getUpperEstimate(), 0.5));
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
        return this.correctnessProb == 1d && this.lowerEstimate == this.upperEstimate && this.upperEstimate == exactEstimate;
    }

    /**
     * Creates a new instance that represents the sum of the {@code this} and {@code that} instance.
     *
     * @param that the other summand
     * @return the sum
     */
    public ProbabilisticDoubleInterval plus(ProbabilisticDoubleInterval that) {
        return new ProbabilisticDoubleInterval(
                this.getLowerEstimate() + that.getLowerEstimate(),
                this.getUpperEstimate() + that.getUpperEstimate(),
                Math.min(this.getCorrectnessProbability(), that.getCorrectnessProbability())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        ProbabilisticDoubleInterval estimate = (ProbabilisticDoubleInterval) o;
        return Double.compare(estimate.correctnessProb, this.correctnessProb) == 0 &&
                this.lowerEstimate == estimate.lowerEstimate &&
                this.upperEstimate == estimate.upperEstimate;
    }

    /**
     * Compares with this instance equals with {@code that} instance within given delta bounds.
     */
    public boolean equalsWithinDelta(ProbabilisticDoubleInterval that,
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
        return String.format("(%,.2f..%,.2f ~ %.1f%%)",
                this.lowerEstimate, this.upperEstimate, this.correctnessProb * 100d);
    }

}
