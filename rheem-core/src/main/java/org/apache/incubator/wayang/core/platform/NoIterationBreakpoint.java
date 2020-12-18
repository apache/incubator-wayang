package io.rheem.rheem.core.platform;

import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.ExecutionStage;
import io.rheem.rheem.core.plan.executionplan.ExecutionStageLoop;

/**
 * This {@link Breakpoint} implementation always requests a break unless inside of {@link ExecutionStageLoop}s.
 */
public class NoIterationBreakpoint implements Breakpoint {

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage, ExecutionState state, OptimizationContext context) {
        // TODO: We could break, if we enter a loop, however, multi-stage loop heads have feedback predecessors.
        return stage.getLoop() != null && stage.getPredecessors().stream().anyMatch(
                predecessor -> predecessor.getLoop() != null
        );
    }

}
