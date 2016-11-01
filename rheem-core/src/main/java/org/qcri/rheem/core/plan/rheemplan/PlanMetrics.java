package org.qcri.rheem.core.plan.rheemplan;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;

import java.util.Collection;

/**
 * This class collects metrics for {@link RheemPlan}s.
 */
@Type("planmetrics")
public class PlanMetrics extends Measurement {

    /**
     * Creates a new instance and collects metrics from the given {@link RheemPlan}.
     *
     * @param rheemPlan the {@link RheemPlan}
     * @param id        for the new instance
     * @return the new instance
     */
    public static PlanMetrics createFor(RheemPlan rheemPlan, String id) {
        PlanMetrics planMetrics = new PlanMetrics(id);
        planMetrics.collectFrom(rheemPlan);
        return planMetrics;
    }

    /**
     * Number of virtual {@link Operator}s, {@link ExecutionOperator}s, and {@link OperatorAlternative}s.
     */
    private int numVirtualOperators, numExecutionOperators, numAlternatives;

    /**
     * Number of possible combinations of {@link ExecutionOperator}s in the {@link RheemPlan}.
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
     * @param rheemPlan on which the metrics should be collected
     */
    private void collectFrom(RheemPlan rheemPlan) {
        final Collection<Operator> operators = PlanTraversal.upstream().traverse(rheemPlan.getSinks()).getTraversedNodes();
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
     * Provide the number of virtual {@link Operator}s in the {@link RheemPlan}.
     *
     * @return the number of virtual {@link Operator}s
     */
    public int getNumVirtualOperators() {
        return this.numVirtualOperators;
    }

    /**
     * Provide the number of {@link ExecutionOperator}s in the {@link RheemPlan}.
     *
     * @return the number of {@link ExecutionOperator}s
     */
    public int getNumExecutionOperators() {
        return this.numExecutionOperators;
    }

    /**
     * Provide the number of {@link OperatorAlternative}s in the {@link RheemPlan}.
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
