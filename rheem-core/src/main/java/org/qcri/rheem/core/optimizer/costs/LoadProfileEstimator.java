package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.api.Configuration;

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
     * Nest a sub-instance.
     *
     * @param loadProfileEstimator the sub-instance
     */
    void nest(LoadProfileEstimator loadProfileEstimator);

    /**
     * Retrieve nested instances.
     *
     * @return the nested instances
     */
    Collection<LoadProfileEstimator> getNestedEstimators();

    /**
     * Retrieve the {@link Configuration} key for this instance.
     *
     * @return the key or {@code null} if none
     */
    String getConfigurationKey();

    /**
     * Retrieve the {@link Configuration} template key if any. Usually, this is the {@link Configuration} key
     * suffixed by {@code .template}.
     *
     * @return the template key or {@code null} if none
     */
    default String getTemplateKey() {
        final String configKey = this.getConfigurationKey();
        return configKey == null ? null : configKey + ".template";
    }

}
