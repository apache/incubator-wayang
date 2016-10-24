package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
 */
public class TopKPruningStrategy implements PlanEnumerationPruningStrategy {

    private Comparator<ProbabilisticDoubleInterval> costEstimateComparator;

    private int k;

    @Override
    public void configure(Configuration configuration) {
        this.costEstimateComparator = configuration.getCostEstimateComparatorProvider().provide();
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
        final ProbabilisticDoubleInterval t1 = p1.getCostEstimate(true);
        final ProbabilisticDoubleInterval t2 = p2.getCostEstimate(true);
        return this.costEstimateComparator.compare(t1, t2);
    }

}
