package io.rheem.rheem.core.platform;

import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.ExecutionPlan;
import io.rheem.rheem.core.plan.executionplan.ExecutionStage;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
@FunctionalInterface
public interface Breakpoint {

    /**
     * Tests whether the given {@link ExecutionStage} can be executed.
     *
     * @param stage   whose execution is in question
     * @param state   current {@link ExecutionState}
     * @param context {@link OptimizationContext} of the last optimization process
     * @return whether the {@link ExecutionStage} should be executed
     */
    boolean permitsExecutionOf(ExecutionStage stage,
                               ExecutionState state,
                               OptimizationContext context);

    /**
     * {@link Breakpoint} implementation that never breaks.
     */
    Breakpoint NONE = (stage, state, optCtx) -> true;

}
