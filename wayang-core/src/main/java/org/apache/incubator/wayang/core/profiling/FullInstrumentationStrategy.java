package org.apache.incubator.wayang.core.profiling;

import org.apache.incubator.wayang.core.plan.executionplan.Channel;
import org.apache.incubator.wayang.core.plan.executionplan.ExecutionStage;

import java.util.Arrays;

/**
 * Instruments only outbound {@link Channel}s.
 */
public class FullInstrumentationStrategy implements InstrumentationStrategy {

    @Override
    public void applyTo(ExecutionStage stage) {
        stage.getAllTasks().stream()
                .flatMap(task -> Arrays.stream(task.getOutputChannels()))
                .forEach(Channel::markForInstrumentation);
    }
}
