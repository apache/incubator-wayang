package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.
 */
public class PlatformExecution {

    private Collection<ExecutionStage> stages = new LinkedList<>();

    private final Platform platform;

    public PlatformExecution(Platform platform) {
        this.platform = platform;
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

    public ExecutionStage createStage(int sequenceNumber) {
        return new ExecutionStage(this, sequenceNumber);
    }

    public ExecutionStage createStage() {
        return this.createStage(-1);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.platform);
    }
}
