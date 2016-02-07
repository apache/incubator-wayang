package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

import java.util.HashMap;
import java.util.Map;

/**
 * Mock-up for a repository that manages {@link CardinalityEstimator}s and the like.
 */
public class CardinalityEstimatorManager {

    private final Configuration configuration;

    private final Map<OutputSlot<?>, CardinalityEstimate> cache = new HashMap<>();

    public CardinalityEstimatorManager(Configuration configuration) {
        this.configuration = configuration;
    }

    public void pushCardinalityEstimation(RheemPlan rheemPlan) {
        final CardinalityPusher pusher = CompositeCardinalityPusher.createFor(rheemPlan, this.configuration, this.cache);
        pusher.push(this.configuration, new CardinalityEstimate[0]);
    }

    public Map<OutputSlot<?>, CardinalityEstimate> getCache() {
        return cache;
    }
}
