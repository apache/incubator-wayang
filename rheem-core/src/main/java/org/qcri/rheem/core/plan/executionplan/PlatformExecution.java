package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.
 */
public class PlatformExecution {

    private static final AtomicInteger SEQUENCE_NUMBER_GENERATOR = new AtomicInteger(0);

    private final int sequenceNumber;

    private Collection<ExecutionStage> stages = new LinkedList<>();

    private final Platform platform;

    public PlatformExecution(Platform platform) {
        this.platform = platform;
        this.sequenceNumber = SEQUENCE_NUMBER_GENERATOR.getAndIncrement();
    }

    void addStage(ExecutionStage stage) {
        Validate.isTrue(stage.getPlatformExecution() == this);
        this.stages.add(stage);
    }

    public Collection<ExecutionStage> getStages() {
        return this.stages;
    }

    public Platform getPlatform() {
        return this.platform;
    }

    public ExecutionStage createStage(ExecutionStageLoop executionStageLoop, int sequenceNumber) {
        return new ExecutionStage(this, executionStageLoop, sequenceNumber);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.platform);
    }

    int getSequenceNumber() {
        return this.sequenceNumber;
    }

    public void retain(Set<ExecutionStage> retainableStages) {
        this.stages.retainAll(retainableStages);
    }
}
