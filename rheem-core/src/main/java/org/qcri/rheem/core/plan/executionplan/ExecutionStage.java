package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Resides within a {@link PlatformExecution} and represents the minimum execution unit that is controlled by Rheem.
 * <p>The purpose of stages is to allow to do only a part of work that is to be done by a single
 * {@link PlatformExecution} and invoke a further {@link PlatformExecution} to proceed working with the results
 * of this stage. Also, this allows to consume data with a {@link PlatformExecution} only when it is needed, i.e.,
 * at a deferred stage. However, the level of control that can be imposed by Rheem can vary between {@link Platform}s</p>
 */
public class ExecutionStage {

    /**
     * Where this instance resides in.
     */
    private final PlatformExecution platformExecution;

    /**
     * Directly preceding instances (have to be executed before this instance).
     */
    private final Collection<ExecutionStage> predecessors = new LinkedList<>();

    /**
     * Directly succeeding instances (have to be executed after this instance).
     */
    private final Collection<ExecutionStage> successors = new LinkedList<>();

    /**
     * Tasks that have to be done first when processing this instance.
     */
    private final Collection<ExecutionTask> startTasks = new LinkedList<>();

    /**
     * Tasks that have to be done last when processing this instance.
     */
    private final Collection<ExecutionTask> terminalTasks = new LinkedList<>();

    /**
     * For printing and debugging purposes only.
     */
    private final int sequenceNumber;

    /**
     * Create a new instance and register it with the given {@link PlatformExecution}.
     */
    ExecutionStage(PlatformExecution platformExecution, int sequenceNumber) {
        this.platformExecution = platformExecution;
        this.sequenceNumber = sequenceNumber;
        this.platformExecution.addStage(this);
    }

    /**
     * Mutually register a predecessor/successor relationship among this and the given instance.
     *
     * @param that a new successor of this instance
     */
    public void addSuccessor(ExecutionStage that) {
        this.successors.add(that);
        that.predecessors.add(this);
    }

    public PlatformExecution getPlatformExecution() {
        return this.platformExecution;
    }

    public Collection<ExecutionStage> getPredecessors() {
        return this.predecessors;
    }

    public Collection<ExecutionStage> getSuccessors() {
        return this.successors;
    }

    public void addTask(ExecutionTask task) {
        task.setStage(this);
    }

    public void markAsStartTast(ExecutionTask executionTask) {
        Validate.isTrue(executionTask.getStage() == this);
        this.startTasks.add(executionTask);
    }

    public void markAsTerminalTask(ExecutionTask executionTask) {
        Validate.isTrue(executionTask.getStage() == this);
        this.terminalTasks.add(executionTask);
    }

    public Collection<ExecutionTask> getStartTasks() {
        return this.startTasks;
    }

    public boolean isStartingStage() {
        return this.predecessors.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("%s[%s:%d]", this.getClass().getSimpleName(), this.platformExecution.getPlatform().getName(),
                this.sequenceNumber);
    }

    public Collection<ExecutionTask> getTerminalTasks() {
        return terminalTasks;
    }
}
