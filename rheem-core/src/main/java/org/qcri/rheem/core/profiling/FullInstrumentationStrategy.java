package org.qcri.rheem.core.profiling;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;

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
