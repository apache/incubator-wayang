package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.Slot;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This {@link PlanEnumerationPruningStrategy} follows the idea that we can throw away a
 * {@link PlanEnumeration.PartialPlan}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class InternalOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    private final Map<ExecutionOperator, TimeEstimate> cachedTimeEstimates;

    private final Comparator<TimeEstimate> timeEstimateComparator;

    public InternalOperatorPruningStrategy(Map<ExecutionOperator, TimeEstimate> cachedTimeEstimates,
                                           Comparator<TimeEstimate> timeEstimateComparator) {
        this.cachedTimeEstimates = cachedTimeEstimates;
        this.timeEstimateComparator = timeEstimateComparator;
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Group plans.
        final Collection<List<PlanEnumeration.PartialPlan>> competingPlans =
                planEnumeration.getPartialPlans().stream()
                        .collect(Collectors.groupingBy(PlanEnumeration.PartialPlan::getInterfaceOperators))
                        .values();
        final List<PlanEnumeration.PartialPlan> bestPlans = competingPlans.stream()
                .map(this::selectBestPlan)
                .collect(Collectors.toList());
        planEnumeration.getPartialPlans().retainAll(bestPlans);
    }

    private PlanEnumeration.PartialPlan selectBestPlan(List<PlanEnumeration.PartialPlan> partialPlan) {
        return partialPlan.stream().reduce(this::selectBestPlan).get();
    }

    private PlanEnumeration.PartialPlan selectBestPlan(PlanEnumeration.PartialPlan p1, PlanEnumeration.PartialPlan p2) {
        final TimeEstimate t1 = p1.getExecutionTimeEstimate(this.cachedTimeEstimates);
        final TimeEstimate t2 = p2.getExecutionTimeEstimate(this.cachedTimeEstimates);
        return this.timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
    }

}
