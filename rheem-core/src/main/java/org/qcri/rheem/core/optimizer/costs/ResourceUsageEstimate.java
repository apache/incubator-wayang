package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;

/**
 * An estimate of costs of some executable code expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class ResourceUsageEstimate extends ProbabilisticIntervalEstimate {

    public ResourceUsageEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }

}
