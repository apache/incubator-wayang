package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;

/**
 * A strategy to prune {@link PartialPlan}s from a {@link PlanEnumeration}.
 */
@FunctionalInterface
public interface PlanEnumerationPruningStrategy {

    void prune(PlanEnumeration planEnumeration, Configuration configuration);

}
