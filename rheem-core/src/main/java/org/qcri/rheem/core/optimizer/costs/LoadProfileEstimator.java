package org.qcri.rheem.core.optimizer.costs;

/**
 * Estimates the {@link LoadProfile} of some executable artifact that takes some input data quanta and produces them.
 */
public interface LoadProfileEstimator {

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param context provides parameters for the estimation
     * @return the {@link LoadProfile}
     */
    LoadProfile estimate(EstimationContext context);

}
