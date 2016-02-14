package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.util.Counter;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

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
        return this.startingStages;
    }

    public String toExtensiveString() {
        StringBuilder sb = new StringBuilder();
        Counter<ExecutionStage> stageActivationCounter = new Counter<>();
        Queue<ExecutionStage> activatedStages = new LinkedList<>(this.startingStages);
        while (!activatedStages.isEmpty()) {
            while (!activatedStages.isEmpty()) {
                final ExecutionStage stage = activatedStages.poll();
                sb.append(">>> ").append(stage).append(":\n");
                stage.toExtensiveString(sb);
                sb.append("\n");

                for (ExecutionStage successor : stage.getSuccessors()) {
                    final int count = stageActivationCounter.add(successor, 1);
                    if (count == successor.getPredecessors().size()) {
                        activatedStages.add(successor);
                    }
                }
            }
        }
        return sb.toString();
    }
}
