package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.util.Formats;

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

    public TimeEstimate plus(long millis) {
        return new TimeEstimate(
                this.getLowerEstimate() + millis,
                this.getUpperEstimate() + millis,
                this.getCorrectnessProbability()
        );
    }

    public TimeEstimate times(double scalar) {
        return scalar == 1d ? this : new TimeEstimate(
                Math.round(this.getLowerEstimate() * scalar),
                Math.round(this.getUpperEstimate() * scalar),
                this.getCorrectnessProbability()
        );
    }

    @Override
    public String toString() {
        return this.toIntervalString(false);
//        return toGMeanString();
    }

    @SuppressWarnings("unused")
    public String toIntervalString(boolean isProvideRaw) {
        return String.format("(%s .. %s, p=%s)",
                Formats.formatDuration(this.getLowerEstimate(), isProvideRaw),
                Formats.formatDuration(this.getUpperEstimate(), isProvideRaw),
                Formats.formatPercentage(this.getCorrectnessProbability()));
    }

    @SuppressWarnings("unused")
    public String toGMeanString() {
        final long geometricMeanEstimate = this.getGeometricMeanEstimate();
        final double dev = geometricMeanEstimate == 0 ? 0d : this.getUpperEstimate() / (double) geometricMeanEstimate;
        return String.format("(%s, d=%.1f, p=%s)",
                Formats.formatDuration(geometricMeanEstimate, true),
                dev,
                Formats.formatPercentage(this.getCorrectnessProbability()));
    }
}
