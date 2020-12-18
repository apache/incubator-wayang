package org.apache.incubator.wayang.core.platform;

import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.incubator.wayang.core.plan.executionplan.ExecutionStage;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
public class FixBreakpoint implements Breakpoint {

    /**
     * {@link ExecutionStage}s that should not yet be executed.
     */
    private final Set<ExecutionStage> stagesToSuspend = new HashSet<>();

    public FixBreakpoint breakAfter(ExecutionStage stage) {
        return this.breakBefore(stage.getSuccessors());
    }

    public FixBreakpoint breakBefore(ExecutionStage stage) {
        return this.breakBefore(Collections.singleton(stage));
    }

    public FixBreakpoint breakBefore(Collection<ExecutionStage> stages) {
        for (ExecutionStage stage : stages) {
            if (this.stagesToSuspend.add(stage)) {
                this.breakBefore(stage.getSuccessors());
            }
        }
        return this;
    }

    @Override
    public boolean permitsExecutionOf(ExecutionStage stage,
                                      ExecutionState state,
                                      OptimizationContext optimizationContext) {
        return !this.stagesToSuspend.contains(stage);
    }

}
