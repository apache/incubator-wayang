package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

import java.util.HashSet;
import java.util.Set;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
@FunctionalInterface
public interface Breakpoint {

    boolean permitsExecutionOf(ExecutionStage stage);

    /**
     * Tests whether the given {@link ExecutionStage} can be executed.
     * @param stage whose execution is in question
     * @return whether the {@link ExecutionStage} should <b>not</b> be executed
     */
    default boolean requestsBreakBefore(ExecutionStage stage) {
        return !this.permitsExecutionOf(stage);
    }

    /**
     * {@link Breakpoint} implementation that never breaks.
     */
    Breakpoint NONE = stage -> true;

}
