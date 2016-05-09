package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.platform.Platform;

import java.util.*;
import java.util.stream.Collectors;

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
    private final Set<ExecutionStage> successors = new HashSet<>();

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
     * Tells whether this instance has already been put into execution.
     */
    private boolean wasExecuted = false;

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
        if (this.successors.add(that)) {
            that.predecessors.add(this);
        }
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

    public void markAsStartTask(ExecutionTask executionTask) {
        Validate.isTrue(executionTask.getStage() == this);
        this.startTasks.add(executionTask);
    }

    public void markAsTerminalTask(ExecutionTask executionTask) {
        Validate.isTrue(executionTask.getStage() == this);
        this.terminalTasks.add(executionTask);
    }

    /**
     * All tasks with exclusively inbound input {@link Channel}s
     */
    public Collection<ExecutionTask> getStartTasks() {
        return this.startTasks;
    }

    public boolean isStartingStage() {
        return this.predecessors.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("%s[%s-%d:%d-%6x]",
                this.getClass().getSimpleName(),
                this.platformExecution.getPlatform().getName(),
                this.platformExecution.getSequenceNumber(),
                this.sequenceNumber,
                this.hashCode());
    }

    /**
     * All tasks with exclusively outbound output {@link Channel}s
     */
    public Collection<ExecutionTask> getTerminalTasks() {
        return terminalTasks;
    }

    /**
     * @return all {@link Channel}s of this instance that connect to other {@link ExecutionStage}s
     */
    public Collection<Channel> getOutboundChannels() {
        return this.getAllTasks().stream()
                .flatMap(
                        task -> Arrays.stream(task.getOutputChannels()).filter(Channel::isBetweenStages)
                ).collect(Collectors.toList());

    }

    /**
     * @return all {@link Channel}s of this instance that connect from other {@link ExecutionStage}s
     */
    public Collection<Channel> getInboundChannels() {
        return this.getAllTasks().stream()
                .flatMap(task ->
                        Arrays.stream(task.getInputChannels()).filter(
                                channel -> channel.getProducer().getStage() != this
                        )
                ).collect(Collectors.toList());

    }

    public String toExtensiveString() {
        final StringBuilder sb = new StringBuilder();
        this.toExtensiveString(sb);
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Appends this instance's details to the given {@link StringBuilder}.
     */
    public void toExtensiveString(StringBuilder sb) {
        Set<ExecutionTask> seenTasks = new HashSet<>();
        for (ExecutionTask startTask : this.startTasks) {
            for (Channel inputChannel : startTask.getInputChannels()) {
                sb.append(this.prettyPrint(inputChannel))
                        .append(" => ")
                        .append(this.prettyPrint(startTask)).append('\n');
            }
            this.toExtensiveStringAux(startTask, seenTasks, sb);
        }
    }

    private void toExtensiveStringAux(ExecutionTask task, Set<ExecutionTask> seenTasks, StringBuilder sb) {
        if (!seenTasks.add(task)) {
            return;
        }
        for (Channel channel : task.getOutputChannels()) {
            for (ExecutionTask consumer : channel.getConsumers()) {
                if (consumer.getStage() == this) {
                    sb.append(this.prettyPrint(task))
                            .append(" => ")
                            .append(this.prettyPrint(channel))
                            .append(" => ")
                            .append(this.prettyPrint(consumer)).append('\n');
                    this.toExtensiveStringAux(consumer, seenTasks, sb);
                } else {
                    sb.append(this.prettyPrint(task))
                            .append(" => ")
                            .append(this.prettyPrint(channel)).append('\n');
                }
            }
        }
    }

    private String prettyPrint(Channel channel) {
        return channel.getClass().getSimpleName();
    }

    private String prettyPrint(ExecutionTask task) {
        return task.getOperator().toString();
    }

    /**
     * Collects all {@link ExecutionTask}s of this instance.
     */
    public Set<ExecutionTask> getAllTasks() {
        final Queue<ExecutionTask> nextTasks = new LinkedList<>(this.startTasks);
        final Set<ExecutionTask> allTasks = new HashSet<>();

        while (!nextTasks.isEmpty()) {
            final ExecutionTask task = nextTasks.poll();
            assert task.getStage() == this;
            if (allTasks.add(task) && !this.terminalTasks.contains(task)) {
                Arrays.stream(task.getOutputChannels())
                        .flatMap(channel -> channel.getConsumers().stream())
                        .filter(consumer -> consumer.getStage() == this)
                        .forEach(nextTasks::add);
            }
        }
        return allTasks;
    }

    public void retainSuccessors(Set<ExecutionStage> retainableStages) {
        for (Iterator<ExecutionStage> i = this.successors.iterator(); i.hasNext(); ) {
            final ExecutionStage successor = i.next();
            if (!retainableStages.contains(successor)) {
                i.remove();
                successor.predecessors.remove(this);
            }
        }
    }

    public boolean wasExecuted() {
        return this.wasExecuted;
    }

    public void setWasExecuted(boolean wasExecuted) {
        this.wasExecuted = wasExecuted;
    }
}
