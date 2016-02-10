package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Complete data flow on a single platform, that consists of multiple {@link ExecutionStage}s.
 */
public class PlatformExecution {

    private Collection<ExecutionStage> stages = new LinkedList<>();

    void addStage(ExecutionStage stage) {
        Validate.isTrue(stage.getPlatformExecution() == this);
        this.stages.add(stage);
    }

    public Collection<ExecutionStage> getStages() {
        return this.stages;
    }
}
