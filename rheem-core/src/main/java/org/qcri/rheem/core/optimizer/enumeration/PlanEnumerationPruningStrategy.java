package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;

/**
 * A strategy to prune {@link PlanImplementation}s from a {@link PlanEnumeration}.
 */
public interface PlanEnumerationPruningStrategy {

    /**
     * Configure this instance. Should be called (once) before using {@link #prune(PlanEnumeration)}.
     *
     * @param configuration provides configuration values
     */
    void configure(Configuration configuration);

    /**
     * Prune down the {@link PlanEnumeration}, i.e., remove some of its {@link PlanImplementation}s.
     *
     * @param planEnumeration to be pruned
     */
    void prune(PlanEnumeration planEnumeration);
}
