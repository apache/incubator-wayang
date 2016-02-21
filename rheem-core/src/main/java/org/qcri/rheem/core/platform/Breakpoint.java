package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

import java.util.HashSet;
import java.util.Set;

/**
 * Describes when to interrupt the execution of an {@link ExecutionPlan}.
 */
public interface Breakpoint {

    boolean permitsExecutionOf(ExecutionStage stage);

}
