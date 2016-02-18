package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

import java.util.HashSet;
import java.util.Set;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
public class Breakpoint {

    /**
     * {@link ExecutionStage}s that should not yet be executed.
     */
    private final Set<ExecutionStage> targetStages = new HashSet<>();

    public Breakpoint breakAfter(ExecutionStage stage) {
        this.targetStages.addAll(stage.getSuccessors());
        return this;
    }

    public Breakpoint breakAt(ExecutionStage stage) {
        this.targetStages.add(stage);
        return this;
    }

    public boolean permitsExecutionOf(ExecutionStage stage) {
        return !this.targetStages.contains(stage);
    }

}
