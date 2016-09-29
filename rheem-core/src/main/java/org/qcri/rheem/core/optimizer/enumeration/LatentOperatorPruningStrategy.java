package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Slot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link PlanEnumerationPruningStrategy} follows the idea that we can prune a
 * {@link PlanImplementation}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class LatentOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    private static final Logger logger = LoggerFactory.getLogger(LatentOperatorPruningStrategy.class);

    private Comparator<ProbabilisticDoubleInterval> costEstimateComparator;

    @Override
    public void configure(Configuration configuration) {
        this.costEstimateComparator = configuration.getCostEstimateComparatorProvider().provide();
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Group plans.
        final Collection<List<PlanImplementation>> competingPlans =
                planEnumeration.getPlanImplementations().stream()
                        .collect(Collectors.groupingBy(LatentOperatorPruningStrategy::getInterestingProperties))
                        .values();
        final List<PlanImplementation> bestPlans = competingPlans.stream()
                .map(plans -> this.selectBestPlanNary(plans, this.costEstimateComparator))
                .collect(Collectors.toList());
        planEnumeration.getPlanImplementations().retainAll(bestPlans);
    }

    /**
     * Extracts the interesting properties of a {@link PlanImplementation}.
     *
     * @param implementation whose interesting properties are requested
     * @return the interesting properties of the given {@code implementation}
     */
    private static Tuple<Set<Platform>, Collection<ExecutionOperator>> getInterestingProperties(PlanImplementation implementation) {
        return new Tuple<>(
                implementation.getUtilizedPlatforms(),
                implementation.getInterfaceOperators()
        );
    }

    private PlanImplementation selectBestPlanNary(List<PlanImplementation> planImplementation,
                                                  Comparator<ProbabilisticDoubleInterval> costEstimateComparator) {
        assert !planImplementation.isEmpty();
        return planImplementation.stream()
                .reduce((plan1, plan2) -> this.selectBestPlanBinary(plan1, plan2, costEstimateComparator))
                .get();
    }

    private PlanImplementation selectBestPlanBinary(PlanImplementation p1,
                                                    PlanImplementation p2,
                                                    Comparator<ProbabilisticDoubleInterval> costEstimateComparator) {
        final ProbabilisticDoubleInterval t1 = p1.getCostEstimate(true);
        final ProbabilisticDoubleInterval t2 = p2.getCostEstimate(true);
        final boolean isPickP1 = costEstimateComparator.compare(t1, t2) <= 0;
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
