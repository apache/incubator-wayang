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
 * {@link PlanImplementation}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class InternalOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    @Override
    public void prune(PlanEnumeration planEnumeration, Configuration configuration) {
        // Group plans.
        final Collection<List<PlanImplementation>> competingPlans =
                planEnumeration.getPlanImplementations().stream()
                        .collect(Collectors.groupingBy(PlanImplementation::getInterfaceOperators))
                        .values();
        final Comparator<TimeEstimate> timeEstimateComparator = configuration.getTimeEstimateComparatorProvider().provide();
        final List<PlanImplementation> bestPlans = competingPlans.stream()
                .map(plans -> this.selectBestPlanNary(plans, timeEstimateComparator))
                .collect(Collectors.toList());
        planEnumeration.getPlanImplementations().retainAll(bestPlans);
    }

    private PlanImplementation selectBestPlanNary(List<PlanImplementation> planImplementation,
                                                  Comparator<TimeEstimate> timeEstimateComparator) {
        return planImplementation.stream()
                .reduce((plan1, plan2) -> this.selectBestPlanBinary(plan1, plan2, timeEstimateComparator))
                .get();
    }

    private PlanImplementation selectBestPlanBinary(PlanImplementation p1,
                                                    PlanImplementation p2,
                                                    Comparator<TimeEstimate> timeEstimateComparator) {
        final TimeEstimate t1 = p1.getTimeEstimate();
        final TimeEstimate t2 = p2.getTimeEstimate();
        return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
    }

}
