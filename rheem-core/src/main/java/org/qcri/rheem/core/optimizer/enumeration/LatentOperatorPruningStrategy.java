package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This {@link PlanEnumerationPruningStrategy} follows the idea that we can prune a
 * {@link PlanImplementation}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class LatentOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    private static final Logger logger = LoggerFactory.getLogger(LatentOperatorPruningStrategy.class);

    private Comparator<TimeEstimate> timeEstimateComparator;

    @Override
    public void configure(Configuration configuration) {
        this.timeEstimateComparator = configuration.getTimeEstimateComparatorProvider().provide();
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Group plans.
        final Collection<List<PlanImplementation>> competingPlans =
                planEnumeration.getPlanImplementations().stream()
                        .collect(Collectors.groupingBy(PlanImplementation::getInterfaceOperators))
                        .values();
        final List<PlanImplementation> bestPlans = competingPlans.stream()
                .map(plans -> this.selectBestPlanNary(plans, this.timeEstimateComparator))
                .collect(Collectors.toList());
        planEnumeration.getPlanImplementations().retainAll(bestPlans);
    }

    private PlanImplementation selectBestPlanNary(List<PlanImplementation> planImplementation,
                                                  Comparator<TimeEstimate> timeEstimateComparator) {
        assert !planImplementation.isEmpty();
        return planImplementation.stream()
                .reduce((plan1, plan2) -> this.selectBestPlanBinary(plan1, plan2, timeEstimateComparator))
                .get();
    }

    private PlanImplementation selectBestPlanBinary(PlanImplementation p1,
                                                    PlanImplementation p2,
                                                    Comparator<TimeEstimate> timeEstimateComparator) {
        final TimeEstimate t1 = p1.getTimeEstimate();
        final TimeEstimate t2 = p2.getTimeEstimate();
        final boolean isPickP1 = timeEstimateComparator.compare(t1, t2) <= 0;
        if (logger.isDebugEnabled()) {
            if (isPickP1) {
                LoggerFactory.getLogger(LatentOperatorPruningStrategy.class).debug(
                        "{} < {}: Choosing {} over {}.", p1.getTimeEstimate(), p2.getTimeEstimate(), p1.getOperators(), p2.getOperators()
                );
            } else {
                LoggerFactory.getLogger(LatentOperatorPruningStrategy.class).debug(
                        "{} < {}: Choosing {} over {}.", p2.getTimeEstimate(), p1.getTimeEstimate(), p2.getOperators(), p1.getOperators()
                );
            }
        }
        return isPickP1 ? p1 : p2;
    }

}
