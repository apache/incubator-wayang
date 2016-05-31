package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
 */
public class TopKPruningStrategy implements PlanEnumerationPruningStrategy {

    private Comparator<TimeEstimate> timeEstimateComparator;

    private int k;

    @Override
    public void configure(Configuration configuration) {
        this.timeEstimateComparator = configuration.getTimeEstimateComparatorProvider().provide();
        this.k = (int) configuration.getLongProperty("rheem.core.optimizer.pruning.topk", 5);
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        if (planEnumeration.getPlanImplementations().size() <= this.k) return;

        ArrayList<PlanImplementation> planImplementations = new ArrayList<>(planEnumeration.getPlanImplementations());
        planImplementations.sort(this::comparePlanImplementations);
        planEnumeration.getPlanImplementations().retainAll(planImplementations.subList(0, this.k));
    }


    private int comparePlanImplementations(PlanImplementation p1,
                                           PlanImplementation p2) {
        final TimeEstimate t1 = p1.getTimeEstimate();
        final TimeEstimate t2 = p2.getTimeEstimate();
        return this.timeEstimateComparator.compare(t1, t2);
    }

}
