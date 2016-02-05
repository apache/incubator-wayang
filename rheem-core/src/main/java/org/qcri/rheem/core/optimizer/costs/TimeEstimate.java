package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;

import java.util.Comparator;

/**
 * An estimate of time (in <b>milliseconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class TimeEstimate extends ProbabilisticIntervalEstimate {

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

    public static final Comparator<TimeEstimate> expectionValueComparator() {
        return (t1, t2) -> Long.compare(t1.getAverageEstimate(), t2.getAverageEstimate());
    }

}
