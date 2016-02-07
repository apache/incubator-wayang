package org.qcri.rheem.core.plan.executionplan;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Represents an executable, cross-platform data flow. Consists of muliple {@link PlatformExecution}s.
 */
public class ExecutionPlan {

    /**
     * All {@link ExecutionStage}s without predecessors that need be executed at first.
     */
    private Collection<ExecutionStage> startingStages = new LinkedList<>();

    public void addStartingStage(ExecutionStage executionStage) {
        this.startingStages.add(executionStage);
    }

    public Collection<ExecutionStage> getStartingStages() {
        return startingStages;
    }
}
