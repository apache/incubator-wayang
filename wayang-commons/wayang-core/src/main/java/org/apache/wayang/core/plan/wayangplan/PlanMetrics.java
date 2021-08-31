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

package org.apache.wayang.core.plan.wayangplan;


import java.util.Collection;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Type;

/**
 * This class collects metrics for {@link WayangPlan}s.
 */
@Type("planmetrics")
public class PlanMetrics extends Measurement {

    /**
     * Creates a new instance and collects metrics from the given {@link WayangPlan}.
     *
     * @param wayangPlan the {@link WayangPlan}
     * @param id        for the new instance
     * @return the new instance
     */
    public static PlanMetrics createFor(WayangPlan wayangPlan, String id) {
        PlanMetrics planMetrics = new PlanMetrics(id);
        planMetrics.collectFrom(wayangPlan);
        return planMetrics;
    }

    /**
     * Number of virtual {@link Operator}s, {@link ExecutionOperator}s, and {@link OperatorAlternative}s.
     */
    private int numVirtualOperators, numExecutionOperators, numAlternatives;

    /**
     * Number of possible combinations of {@link ExecutionOperator}s in the {@link WayangPlan}.
     */
    private long numCombinations;

    /**
     * Serialization constructor.
     */
    protected PlanMetrics() {
    }

    /**
     * Creates a new instance.
     *
     * @param id the ID for the new instance
     */
    private PlanMetrics(String id) {
        super(id);
    }

    /**
     * Collects metrics.
     *
     * @param wayangPlan on which the metrics should be collected
     */
    private void collectFrom(WayangPlan wayangPlan) {
        final Collection<Operator> operators = PlanTraversal.upstream().traverse(wayangPlan.getSinks()).getTraversedNodes();
        this.numCombinations = this.collectFrom(operators);
    }

    /**
     * Collects metrics.
     *
     * @param operators from that the metrics should be collected
     * @return the number of {@link ExecutionOperator} combinations among the {@code operators}
     */
    private long collectFrom(Collection<Operator> operators) {
        long numCombinations = 1L;
        for (Operator operator : operators) {
            if (operator.isElementary()) {
                if (operator.isExecutionOperator()) {
                    this.numExecutionOperators++;
                } else {
                    this.numVirtualOperators++;
                    numCombinations = 0;
                }

            } else {
                if (operator.isAlternative()) {
                    this.numAlternatives++;
                    long numLocalCombinations = 0;
                    for (OperatorAlternative.Alternative alternative : ((OperatorAlternative) operator).getAlternatives()) {
                        numLocalCombinations += this.collectFrom(alternative);
                    }
                    numCombinations *= numLocalCombinations;
                } else if (operator.isSubplan()) {
                    numCombinations *= this.collectFrom((Subplan) operator);
                }
            }
        }
        return numCombinations;
    }

    /**
     * Collects metrics.
     *
     * @param operatorContainer among whose {@link Operator}s metrics should be collected
     * @return the number of combinations of {@link ExecutionOperator}s within the {@code operatorContainer}
     */
    private long collectFrom(OperatorContainer operatorContainer) {
        return this.collectFrom(operatorContainer.getContainedOperators());
    }

    /**
     * Provide the number of virtual {@link Operator}s in the {@link WayangPlan}.
     *
     * @return the number of virtual {@link Operator}s
     */
    public int getNumVirtualOperators() {
        return this.numVirtualOperators;
    }

    /**
     * Provide the number of {@link ExecutionOperator}s in the {@link WayangPlan}.
     *
     * @return the number of {@link ExecutionOperator}s
     */
    public int getNumExecutionOperators() {
        return this.numExecutionOperators;
    }

    /**
     * Provide the number of {@link OperatorAlternative}s in the {@link WayangPlan}.
     *
     * @return the number of {@link OperatorAlternative}s
     */
    public int getNumAlternatives() {
        return this.numAlternatives;
    }

    public long getNumCombinations() {
        return numCombinations;
    }
}
