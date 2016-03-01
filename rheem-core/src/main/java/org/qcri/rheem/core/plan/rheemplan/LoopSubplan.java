package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.optimizer.OptimizationContext;

import java.util.List;

/**
 * Wraps a loop of {@link Operator}s.
 *
 * @see LoopIsolator
 */
public class LoopSubplan extends Subplan {

    private final LoopHeadOperator loopHead;

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     */
    public static LoopSubplan wrap(LoopHeadOperator loopHead, List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        return new LoopSubplan(loopHead, inputs, outputs, container);
    }

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     *
     * @see #wrap(Operator, Operator)
     * @see #wrap(List, List, OperatorContainer)
     */
    private LoopSubplan(LoopHeadOperator loopHead, List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        super(inputs, outputs, container);
        this.loopHead = loopHead;
    }

    /**
     * @see LoopHeadOperator#getNumExpectedIterations()
     */
    public int getNumExpectedIterations() {
        return this.loopHead.getNumExpectedIterations();
    }

    /**
     * @return the {@link LoopHeadOperator} of this instance
     */
    public LoopHeadOperator getLoopHead() {
        return this.loopHead;
    }

    @Override
    public OptimizationContext getInnerInputOptimizationContext(OptimizationContext outerOptimizationContext) {
        // Retrieve the OptimizationContext of the first iteration -> this where we need to propagate to
        final List<OptimizationContext> nestedLoopCtxs = outerOptimizationContext.getNestedLoopContexts(this);
        return nestedLoopCtxs.get(0);
    }

    @Override
    public OptimizationContext getInnerOutputOptimizationContext(OptimizationContext outerOptimizationContext) {
        // Retrieve the OptimizationContext of the last iteration -> this where we need to propagate to
        final List<OptimizationContext> nestedLoopCtxs = outerOptimizationContext.getNestedLoopContexts(this);
        return nestedLoopCtxs.get(nestedLoopCtxs.size() - 1);
    }
}
