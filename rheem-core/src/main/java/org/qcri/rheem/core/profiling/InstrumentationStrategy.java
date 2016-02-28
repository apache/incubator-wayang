package org.qcri.rheem.core.profiling;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

/**
 * Determines, which {@link Channel}s in an {@link ExecutionPlan} should be instrumented.
 */
public interface InstrumentationStrategy {

    /**
     * Mark {@link Channel}s within the {@code stage} that should be instrumented.
     *
     * @param stage that should be instrumented
     */
    void applyTo(ExecutionStage stage);
}
