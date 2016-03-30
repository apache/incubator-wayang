package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.util.Formats;

import java.util.Comparator;

/**
 * An estimate of time (in <b>milliseconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class TimeEstimate extends ProbabilisticIntervalEstimate {

    public static final TimeEstimate ZERO = new TimeEstimate(0, 0, 1d);

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

    public static final Comparator<TimeEstimate> expectationValueComparator() {
        return (t1, t2) -> Long.compare(t1.getAverageEstimate(), t2.getAverageEstimate());
    }

    @Override
    public String toString() {
        return String.format("%s[%s .. %s, conf=%s]",
                this.getClass().getSimpleName(),
                Formats.formatDuration(this.getLowerEstimate()),
                Formats.formatDuration(this.getUpperEstimate()),
                Formats.formatPercentage(this.getCorrectnessProbability()));
    }
}
