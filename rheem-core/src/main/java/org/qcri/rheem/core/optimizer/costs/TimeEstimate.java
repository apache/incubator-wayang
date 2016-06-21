package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.util.Formats;

import java.util.Comparator;

/**
 * An estimate of time (in <b>milliseconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class TimeEstimate extends ProbabilisticIntervalEstimate {

    public static final TimeEstimate ZERO = new TimeEstimate(0);

    public static final TimeEstimate MINIMUM = new TimeEstimate(1);

    public TimeEstimate(long estimate) {
        super(estimate, estimate, 1d);
    }

    public TimeEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

    public TimeEstimate plus(TimeEstimate that) {
        return new TimeEstimate(
                this.getLowerEstimate() + that.getLowerEstimate(),
                this.getUpperEstimate() + that.getUpperEstimate(),
                Math.min(this.getCorrectnessProbability(), that.getCorrectnessProbability())
        );
    }

    /**
     * Provides a {@link Comparator} for {@link TimeEstimate}s. For two {@link TimeEstimate}s {@code t1} and {@code t2},
     * it works as follows:
     * <ol>
     *     <li>If a either of the {@link TimeEstimate}s has a correctness probability of 0, we consider it to be greater.</li>
     *     <li>Otherwise, we compare the two {@link TimeEstimate}s by their average estimation value.</li>
     * </ol>
     * @return
     */
    public static Comparator<TimeEstimate> expectationValueComparator() {
        return (t1, t2) -> {
            if (t1.getCorrectnessProbability() == 0d) {
                if (t2.getCorrectnessProbability() != 0d) {
                    return 1;
                }
            } else if (t2.getCorrectnessProbability() == 0d) {
                return -1;
            }
            return Long.compare(t1.getAverageEstimate(), t2.getAverageEstimate());
        };
    }

    @Override
    public String toString() {
        return String.format("%s[%s .. %s, conf=%s]",
                this.getClass().getSimpleName(),
                Formats.formatDuration(this.getLowerEstimate(), true),
                Formats.formatDuration(this.getUpperEstimate(), true),
                Formats.formatPercentage(this.getCorrectnessProbability()));
    }

    public TimeEstimate times(double scalar) {
        return scalar == 1d ? this : new TimeEstimate(
                Math.round(this.getLowerEstimate() * scalar),
                Math.round(this.getUpperEstimate() * scalar),
                this.getCorrectnessProbability()
        );
    }
}
