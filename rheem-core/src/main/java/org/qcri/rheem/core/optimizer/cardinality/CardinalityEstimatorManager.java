package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

/**
 * TODO
 */
public class CardinalityEstimatorManager {

    private final Configuration configuration;

    public CardinalityEstimatorManager(Configuration configuration) {
        this.configuration = configuration;
    }

    public void pushCardinalityEstimation(RheemPlan rheemPlan) {
        final CardinalityPusher pusher = CompositeCardinalityPusher.createFor(rheemPlan, this.configuration);
        pusher.push(this.configuration);
    }

}
