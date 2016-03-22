package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;

/**
 * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class LoadEstimate extends ProbabilisticIntervalEstimate {

    public LoadEstimate(long exactValue) {
        this(exactValue, exactValue, 1d);
    }

    public LoadEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

}
