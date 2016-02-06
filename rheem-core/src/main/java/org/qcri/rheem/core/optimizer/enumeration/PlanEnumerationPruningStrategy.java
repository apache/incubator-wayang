package org.qcri.rheem.core.optimizer.enumeration;

import static org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration.*;

/**
 * A strategy to prune {@link PartialPlan}s from a {@link PlanEnumeration}.
 */
@FunctionalInterface
public interface PlanEnumerationPruningStrategy {

    void prune(PlanEnumeration planEnumeration);

}
