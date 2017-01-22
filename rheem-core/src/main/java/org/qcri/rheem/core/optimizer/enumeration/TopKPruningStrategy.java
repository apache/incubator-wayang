package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;

import java.util.ArrayList;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
 */
public class TopKPruningStrategy implements PlanEnumerationPruningStrategy {

    private int k;

    @Override
    public void configure(Configuration configuration) {
        this.k = (int) configuration.getLongProperty("rheem.core.optimizer.pruning.topk", 5);
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Skip if there is nothing to do...
        if (planEnumeration.getPlanImplementations().size() <= this.k) return;

        ArrayList<PlanImplementation> planImplementations = new ArrayList<>(planEnumeration.getPlanImplementations());
        planImplementations.sort(this::comparePlanImplementations);
        planEnumeration.getPlanImplementations().retainAll(planImplementations.subList(0, this.k));
    }


    private int comparePlanImplementations(PlanImplementation p1,
                                           PlanImplementation p2) {
        final double t1 = p1.getSquashedCostEstimate(true);
        final double t2 = p2.getSquashedCostEstimate(true);
        return Double.compare(t1, t2);
    }

}
