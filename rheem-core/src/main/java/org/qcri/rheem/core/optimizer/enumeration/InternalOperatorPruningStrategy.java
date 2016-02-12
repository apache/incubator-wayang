package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.Slot;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@link PlanEnumerationPruningStrategy} follows the idea that we can throw away a
 * {@link PartialPlan}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class InternalOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    @Override
    public void prune(PlanEnumeration planEnumeration, Configuration configuration) {
        // Group plans.
        final Collection<List<PartialPlan>> competingPlans =
                planEnumeration.getPartialPlans().stream()
                        .collect(Collectors.groupingBy(PartialPlan::getInterfaceOperators))
                        .values();
        final Comparator<TimeEstimate> timeEstimateComparator = configuration.getTimeEstimateComparatorProvider().provide();
        final List<PartialPlan> bestPlans = competingPlans.stream()
                .map(plans -> this.selectBestPlanNary(plans, configuration, timeEstimateComparator))
                .collect(Collectors.toList());
        planEnumeration.getPartialPlans().retainAll(bestPlans);
    }

    private PartialPlan selectBestPlanNary(List<PartialPlan> partialPlan,
                                           Configuration configuration,
                                           Comparator<TimeEstimate> timeEstimateComparator) {
        return partialPlan.stream()
                .reduce((plan1, plan2) -> this.selectBestPlanBinary(plan1, plan2, configuration, timeEstimateComparator))
                .get();
    }

    private PartialPlan selectBestPlanBinary(PartialPlan p1,
                                             PartialPlan p2,
                                             Configuration configuration,
                                             Comparator<TimeEstimate> timeEstimateComparator) {
        final TimeEstimate t1 = p1.getExecutionPlan().estimateExecutionTime(configuration);
        final TimeEstimate t2 = p2.getExecutionPlan().estimateExecutionTime(configuration);
        return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
    }

}
