package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;

import java.util.Collection;
import java.util.HashSet;

/**
 * TODO
 */
public class ExecutionStageLoop {

    private final LoopSubplan loopSubplan;

    private ExecutionStage headStageCache;

    private final Collection<ExecutionStage> allStages = new HashSet<>();

    public ExecutionStageLoop(LoopSubplan loopSubplan) {
        this.loopSubplan = loopSubplan;
    }

    public void add(ExecutionStage executionStage) {
        if (this.allStages.add(executionStage)) {
            if (this.headStageCache == null && this.checkForLoopHead(executionStage)) {
                this.headStageCache = executionStage;
            }
        }
    }

    private boolean checkForLoopHead(ExecutionStage executionStage) {
        return executionStage.getAllTasks().stream().anyMatch(this::isLoopHead);
    }

    private boolean isLoopHead(ExecutionTask task) {
        final ExecutionOperator operator = task.getOperator();
        return operator.isLoopHead() && operator.getInnermostLoop() == this.loopSubplan;

    }

    public void update(ExecutionStage executionStage, ExecutionTask task) {
        if (this.headStageCache == null && this.isLoopHead(task)) {
            this.headStageCache = executionStage;
        }
    }
}
