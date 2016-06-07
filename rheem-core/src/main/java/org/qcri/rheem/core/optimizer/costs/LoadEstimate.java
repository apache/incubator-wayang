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

    /**
     * Multiplies the estimated load. The correctness probability is not altered.
     *
     * @param n scalar to multiply with
     * @return the product
     */
    public LoadEstimate times(int n) {
        return new LoadEstimate(this.getLowerEstimate() * n, this.getUpperEstimate() * n, this.getCorrectnessProbability());
    }

}
