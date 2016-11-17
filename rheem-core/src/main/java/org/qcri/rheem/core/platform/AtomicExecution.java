package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

/**
 * An atomic execution describes the smallest work unit considered by Rheem's cost model.
 */
public class AtomicExecution {

    /**
     * Estimates the {@link LoadProfile} of the execution unit.
     */
    private LoadProfileEstimator loadProfileEstimator;

    /**
     * Creates a new instance with the given {@link LoadProfileEstimator}.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator}
     */
    public AtomicExecution(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    /**
     * Estimates the {@link LoadProfile} for this instance under a given {@link EstimationContext}.
     *
     * @param context the {@link EstimationContext}
     * @return the {@link LoadProfile}
     */
    public LoadProfile estimateLoad(EstimationContext context) {
        return this.loadProfileEstimator.estimate(context);
    }
}
