package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class checks a {@link PhysicalPlan} for several sanity criteria:
 * <ol>
 * <li>{@link Subplan}s must only be used as top-level {@link Operator} of {@link OperatorAlternative.Alternative}</li>
 * </ol>
 */
public class SanityChecker {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(SanityChecker.class);

    /**
     * Is subject to the sanity checks.
     */
    private final PhysicalPlan physicalPlan;

    /**
     * Create a new instance
     *
     * @param physicalPlan is subject to sanity checks
     */
    public SanityChecker(PhysicalPlan physicalPlan) {
        this.physicalPlan = physicalPlan;
    }

    public boolean checkAllCriteria() {
        boolean isAllChecksPassed = checkProperSubplans();
        // ...

        return isAllChecksPassed;
    }

    /**
     * Check whether {@link Subplan}s are used properly.
     *
     * @return whether the test passed
     */
    public boolean checkProperSubplans() {
        final AtomicBoolean testOutcome = new AtomicBoolean(true);
        checkProperSubplans(this.physicalPlan.getSinks(), testOutcome);
        return testOutcome.get();
    }

    /**
     * Recursive test for proper usage of {@link Subplan}.
     *
     * @param downstreamOperators {@link Operator}s whose {@link InputSlot}s will be traced to proceed the check
     * @param testOutcome         carries the current test outcome and will be updated on problems
     */
    public void checkProperSubplans(final Collection<Operator> downstreamOperators, final AtomicBoolean testOutcome) {
        new PlanTraversal(true, true)
                .withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                    if (operator.isSubplan()) {
                        this.logger.warn("Improper subplan usage detected at {}.", operator);
                        testOutcome.set(false);
                    } else if (operator.isAlternative()) {
                        final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                        operatorAlternative.getAlternatives().stream()
                                .map(OperatorAlternative.Alternative::getOperator)
                                .filter(Operator::isSubplan)
                                .map((subplan) -> (Subplan) subplan)
                                .forEach(subplan -> this.checkProperSubplans(subplan.collectOutputOperators(), testOutcome));
                    }
                })
                .traverse(downstreamOperators);
    }


}
