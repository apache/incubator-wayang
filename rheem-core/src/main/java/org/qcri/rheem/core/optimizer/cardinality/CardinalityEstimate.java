package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

/**
 * An estimate of cardinality within a {@link RheemPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class CardinalityEstimate extends ProbabilisticIntervalEstimate {

    public static final CardinalityEstimate EMPTY_ESTIMATE = new CardinalityEstimate(0, 0, 1d);

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }
}
