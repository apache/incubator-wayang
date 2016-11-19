package org.qcri.rheem.core.optimizer.costs;

import java.util.Collection;

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

    /**
     * Retrieve nested instances.
     *
     * @return the nested instances
     */
    Collection<LoadProfileEstimator> getNestedEstimators();

    /**
     * Retrieve the {@link org.qcri.rheem.core.api.Configuration} key for this instance.
     *
     * @return the key or {@code null} if none
     */
    String getConfigurationKey();

}
