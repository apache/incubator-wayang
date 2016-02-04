package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;

/**
 * An estimate of time (in <b>seconds</b>) expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class TimeEstimate extends ProbabilisticIntervalEstimate {

    public TimeEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

}
