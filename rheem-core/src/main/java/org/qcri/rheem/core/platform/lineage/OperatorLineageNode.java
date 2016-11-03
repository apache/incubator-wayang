package org.qcri.rheem.core.platform.lineage;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * Encapsulates a {@link OptimizationContext.OperatorContext} in a lazy execution lineage.
 */
public class OperatorLineageNode extends LazyExecutionLineageNode {


    /**
     * The {@link OptimizationContext.OperatorContext} of the encapsulated {@link ExecutionOperator}.
     */
    private final OptimizationContext.OperatorContext operatorContext;

    public OperatorLineageNode(final OptimizationContext.OperatorContext operatorContext) {
        this.operatorContext = operatorContext;
    }

    @Override
    protected <T> T accept(T accumulator, Aggregator<T> aggregator) {
        return aggregator.aggregate(accumulator, this.operatorContext);
    }
}
