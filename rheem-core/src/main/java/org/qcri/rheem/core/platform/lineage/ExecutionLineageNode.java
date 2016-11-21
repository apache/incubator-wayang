package org.qcri.rheem.core.platform.lineage;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.AtomicExecution;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Encapsulates {@link AtomicExecution}s with a common {@link OptimizationContext.OperatorContext} in a lazy execution lineage.
 */
public class ExecutionLineageNode extends LazyExecutionLineageNode {

    /**
     * The {@link OptimizationContext.OperatorContext} of the encapsulated {@link ExecutionOperator}.
     */
    private final OptimizationContext.OperatorContext operatorContext;

    /**
     * The encapsulated {@link AtomicExecution}s.
     */
    private final Collection<AtomicExecution> atomicExecutions;

    public ExecutionLineageNode(final OptimizationContext.OperatorContext estimationContext) {
        this.operatorContext = estimationContext;
        this.atomicExecutions = new LinkedList<>();
    }

    /**
     * Adds an {@link AtomicExecution} to this instance.
     *
     * @param atomicExecution the {@link AtomicExecution}
     * @return this instance
     */
    public ExecutionLineageNode add(AtomicExecution atomicExecution) {
        this.atomicExecutions.add(atomicExecution);
        return this;
    }

    /**
     * Adds an {@link AtomicExecution} to this instance. Short-cut for {@link #add(AtomicExecution)}.
     *
     * @param loadProfileEstimator for which the {@link AtomicExecution} should be added
     * @return this instance
     */
    public ExecutionLineageNode add(LoadProfileEstimator loadProfileEstimator) {
        return this.add(new AtomicExecution(loadProfileEstimator));
    }

    /**
     * Adds an {@link AtomicExecution} for the {@link LoadProfileEstimator} of the described {@link OptimizationContext.OperatorContext}.
     *
     * @return this instance
     */
    public ExecutionLineageNode addAtomicExecutionFromOperatorContext() {
        return this.add(this.operatorContext.getLoadProfileEstimator());
    }

    /**
     * Retrieve the {@link OptimizationContext.OperatorContext} corresponding to this instance.
     *
     * @return the {@link OptimizationContext.OperatorContext}
     */
    public OptimizationContext.OperatorContext getOperatorContext() {
        return this.operatorContext;
    }

    /**
     * Retrieve the encapsulated {@link AtomicExecution}s
     *
     * @return the {@link AtomicExecution}s
     */
    public Collection<AtomicExecution> getAtomicExecutions() {
        return this.atomicExecutions;
    }

    @Override
    protected <T> T accept(T accumulator, Aggregator<T> aggregator) {
        return aggregator.aggregate(accumulator, this);
    }

    @Override
    public String toString() {
        return "ExecutionLineageNode[" + this.operatorContext + ']';
    }
}
