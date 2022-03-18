/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.optimizer;

import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Subplan;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class checks a {@link WayangPlan} for several sanity criteria:
 * <ol>
 * <li>{@link Subplan}s must only be used as top-level {@link Operator} of {@link OperatorAlternative.Alternative}</li>
 * <li>{@link Subplan}s must contain more than one {@link Operator}</li>
 * </ol>
 */
public class SanityChecker {

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger(SanityChecker.class);

    /**
     * Is subject to the sanity checks.
     */
    private final WayangPlan wayangPlan;

    /**
     * Create a new instance
     *
     * @param wayangPlan is subject to sanity checks
     */
    public SanityChecker(WayangPlan wayangPlan) {
        this.wayangPlan = wayangPlan;
    }

    public boolean checkAllCriteria() {
        boolean isAllChecksPassed = this.checkProperSubplans();
        isAllChecksPassed &= this.checkFlatAlternatives();

        return isAllChecksPassed;
    }

    /**
     * Check whether {@link Subplan}s are used properly.
     *
     * @return whether the test passed
     */
    public boolean checkProperSubplans() {
        final AtomicBoolean testOutcome = new AtomicBoolean(true);
        PlanTraversal.upstream()
                .withCallback(this.getProperSubplanCallback(testOutcome))
                .traverse(this.wayangPlan.getSinks());
        return testOutcome.get();
    }

    /**
     * Callback for the recursive test for proper usage of {@link Subplan}.
     *
     * @param testOutcome carries the current test outcome and will be updated on problems
     */
    private PlanTraversal.Callback getProperSubplanCallback(AtomicBoolean testOutcome) {
        return (operator, fromInputSlot, fromOutputSlot) -> {
            if (operator.isSubplan() && !operator.isLoopSubplan()) {
                this.logger.warn("Improper subplan usage detected at {}: not embedded in an alternative.", operator);
                testOutcome.set(false);
                this.checkSubplanNotASingleton((Subplan) operator, testOutcome);
            } else if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                operatorAlternative.getAlternatives().forEach(
                        alternative -> alternative.traverse(this.getProperSubplanCallback(testOutcome))
                );
            }
        };
    }

    /**
     * Check whether the given subplan contains more than one operator.
     *
     * @param subplan     is subject to the check
     * @param testOutcome carries the current test outcome and will be updated on problems
     */
    @SuppressWarnings("unused")
    private void checkSubplanNotASingleton(Subplan subplan, final AtomicBoolean testOutcome) {
        boolean isSingleton = this.traverse(subplan, PlanTraversal.Callback.NOP)
                .getTraversedNodes()
                .size() == 1;
        if (isSingleton) {
            this.logger.warn("Improper subplan usage detected at {}: is a singleton");
            testOutcome.set(false);
        }
    }

    /**
     * TODO: (Documentation) add SanityChecker.checkFlatAlternatives
     *   labels: documentation,todo
     * @return
     */
    public boolean checkFlatAlternatives() {
        AtomicBoolean testOutcome = new AtomicBoolean(true);
        new PlanTraversal(true, false)
                .withCallback(this.getFlatAlternativeCallback(testOutcome))
                .traverse(this.wayangPlan.getSinks());
        return testOutcome.get();
    }

    /**
     * TODO: (Documentation) add SanityChecker.getFlatAlternativeCallback
     *   labels: documentation,todo
     * @return
     */
    private PlanTraversal.Callback getFlatAlternativeCallback(AtomicBoolean testOutcome) {
        return (operator, fromInputSlot, fromOutputSlot) -> {
            if (operator.isAlternative()) {
                final OperatorAlternative operatorAlternative = (OperatorAlternative) operator;
                for (OperatorAlternative.Alternative alternative : operatorAlternative.getAlternatives()) {
                    final Collection<Operator> containedOperators = alternative.getContainedOperators();
                    if (containedOperators.size() == 1) {
                        Operator containedOperator = WayangCollections.getSingle(containedOperators);
                        if (containedOperator.isAlternative()) {
                            this.logger.warn("Improper alternative {}: contains alternatives.", alternative);
                            testOutcome.set(false);
                        }
                    } else {
                        // We could check if there are singleton Subplans with an OperatorAlternative embedded,
                        // but this would violate the singleton Subplan rule anyway.
                        alternative.traverse(this.getFlatAlternativeCallback(testOutcome));
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
