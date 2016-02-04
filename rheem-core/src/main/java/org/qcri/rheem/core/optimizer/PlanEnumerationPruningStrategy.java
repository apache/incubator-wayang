package org.qcri.rheem.core.optimizer;

import static org.qcri.rheem.core.optimizer.PlanEnumeration.*;

/**
 * A strategy to prune {@link PartialPlan}s from a {@link PlanEnumeration}.
 */
@FunctionalInterface
public interface PlanEnumerationPruningStrategy {

    void prune(PlanEnumeration planEnumeration);

}
