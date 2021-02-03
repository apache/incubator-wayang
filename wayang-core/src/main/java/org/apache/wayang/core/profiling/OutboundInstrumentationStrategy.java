package org.apache.wayang.core.profiling;

import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;

/**
 * Instruments only outbound {@link Channel}s.
 */
public class OutboundInstrumentationStrategy implements InstrumentationStrategy {

    @Override
    public void applyTo(ExecutionStage stage) {
        stage.getOutboundChannels().forEach(Channel::markForInstrumentation);
    }
}
