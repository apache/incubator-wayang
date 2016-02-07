package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.rheemplan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class checks a {@link RheemPlan} for several sanity criteria:
 * <ol>
 * <li>{@link Subplan}s must only be used as top-level {@link Operator} of {@link OperatorAlternative.Alternative}</li>
 * <li>{@link Subplan}s must contain more than one {@link Operator}</li>
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
    private final RheemPlan rheemPlan;

    /**
     * Create a new instance
     *
     * @param rheemPlan is subject to sanity checks
     */
    public SanityChecker(RheemPlan rheemPlan) {
        this.rheemPlan = rheemPlan;
    }

    public boolean checkAllCriteria() {
        boolean isAllChecksPassed = checkProperSubplans();
        isAllChecksPassed &= checkFlatAlternatives();

        return isAllChecksPassed;
    }

    /**
     * Check whether {@link Subplan}s are used properly.
     *
     * @return whether the test passed
     */
    public boolean checkProperSubplans() {
        final AtomicBoolean testOutcome = new AtomicBoolean(true);
        new PlanTraversal(true, false)
            .withCallback(getProperSubplanCallback(testOutcome))
            .traverse(rheemPlan.getSinks());
        return testOutcome.get();
    }

    /**
     * Callback for the recursive test for proper usage of {@link Subplan}.
     *
     * @param testOutcome         carries the current test outcome and will be updated on problems
     */
    private PlanTraversal.Callback getProperSubplanCallback(AtomicBoolean testOutcome) {
        return (operator, fromInputSlot, fromOutputSlot) -> {
            if (operator.isSubplan()) {
                this.logger.warn("Improper subplan usage detected at {}: not embedded in an alternative.", operator);
                testOutcome.set(false);
                checkSubplanNotASingleton((Subplan) operator, testOutcome);
            } else if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                operatorAlternative.getAlternatives().stream()
                        .map(OperatorAlternative.Alternative::getOperator)
                        .filter(Operator::isSubplan) // No need to traverse alternatives (must be flat) or single operators.
                        .map((subplan) -> (Subplan) subplan)
                        .forEach(subplan -> {
                            this.checkSubplanNotASingleton(subplan, testOutcome);
                            traverse(subplan, this.getProperSubplanCallback(testOutcome));
                        });
            }
        };
    }

    /**
     * Check whether the given subplan contains more than one operator.
     *
     * @param subplan     is subject to the check
     * @param testOutcome carries the current test outcome and will be updated on problems
     */
    private void checkSubplanNotASingleton(Subplan subplan, final AtomicBoolean testOutcome) {
        boolean isSingleton = traverse(subplan, PlanTraversal.Callback.NOP)
                .getTraversedNodes()
                .size() == 1;
        if (isSingleton) {
            this.logger.warn("Improper subplan usage detected at {}: is a singleton");
            testOutcome.set(false);
        }
    }

    public boolean checkFlatAlternatives() {
        AtomicBoolean testOutcome = new AtomicBoolean(true);
        new PlanTraversal(true, false)
                .withCallback(getFlatAlternativeCallback(testOutcome))
                .traverse(this.rheemPlan.getSinks());
        return testOutcome.get();
    }

    private PlanTraversal.Callback getFlatAlternativeCallback(AtomicBoolean testOutcome) {
        return (operator, fromInputSlot, fromOutputSlot) -> {
            if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {
                    final Operator alternativeOperator = alternative.getOperator();
                    if (alternativeOperator.isAlternative()) {
                        this.logger.warn("Improper alternative {}: contains alternatives.");
                        testOutcome.set(false);
                    } else if (alternativeOperator.isSubplan()) {
                        // We could check if there are singleton Subplans with an OperatorAlternative embedded,
                        // but this would violate the singleton Subplan rule anyway.
                        traverse((Subplan) alternativeOperator, getFlatAlternativeCallback(testOutcome));
                    }
                }
            }
        };
    }

    /**
     * Traverse the nodes of a {@link Subplan} in one direction (depends on if it is a sink or not).
     *
     * @param subplan  is subject to the traversal
     * @param callback is called on each traversed {@link Operator}
     * @return the completed {@link PlanTraversal}
     */
    private PlanTraversal traverse(Subplan subplan, PlanTraversal.Callback callback) {
        if (subplan.isSink()) {
            final Collection<Operator> inputOperators = subplan.collectInputOperators();
            return new PlanTraversal(false, true)
                    .withCallback(callback)
                    .traverse(inputOperators);
        } else {
            final Collection<Operator> outputOperators = subplan.collectOutputOperators();
            return new PlanTraversal(true, false)
                    .withCallback(callback)
                    .traverse(outputOperators);
        }
    }

}
