package org.qcri.rheem.core.profiling;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

/**
 * Instruments only outbound {@link Channel}s.
 */
@SuppressWarnings("unused") // Can be activated via Configuration.
public class NoInstrumentationStrategy implements InstrumentationStrategy {

    @Override
    public void applyTo(ExecutionStage stage) {
        // Nothing will be instrumented.
    }
}
