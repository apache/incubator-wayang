package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resides within a {@link PlatformExecution} and represents the minimum execution unit that is controlled by Rheem.
 * <p>The purpose of stages is to allow to do only a part of work that is to be done by a single
 * {@link PlatformExecution} and invoke a further {@link PlatformExecution} to proceed working with the results
 * of this stage. Also, this allows to consume data with a {@link PlatformExecution} only when it is needed, i.e.,
 * at a deferred stage. However, the level of control that can be imposed by Rheem can vary between {@link Platform}s</p>
 * <p>Note that this class is immutable, i.e., it does not comprise any execution state.</p>
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
     * The loop that this instance is part of or {@code null} if none.
     */
    private final ExecutionStageLoop executionStageLoop;

    /**
     * For printing and debugging purposes only.
     */
    private final int sequenceNumber;


    /**
     * Create a new instance and register it with the given {@link PlatformExecution}.
     */
    ExecutionStage(PlatformExecution platformExecution, ExecutionStageLoop executionStageLoop, int sequenceNumber) {
        this.platformExecution = platformExecution;
        this.sequenceNumber = sequenceNumber;
        this.executionStageLoop = executionStageLoop;
        if (this.executionStageLoop != null) {
            this.executionStageLoop.add(this);
        }
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
        this.updateLoop(task);
    }

    /**
     * Notify the {@link #executionStageLoop} that there is a new {@link ExecutionTask} in this instance, which might
     * comprise the {@link LoopHeadOperator}.
     *
     * @param task
     */
    private void updateLoop(ExecutionTask task) {
        if (this.executionStageLoop != null) {
            this.executionStageLoop.update(task);
        }
    }

    /**
     * Determine if this instance is the loop head of its {@link ExecutionStageLoop}.
     *
     * @return {@code true} if the above condition is fulfilled or there is no {@link ExecutionStageLoop}
     */
    public boolean isLoopHead() {
        return this.executionStageLoop != null && this.executionStageLoop.getLoopHead() == this;
    }

    /**
     * Retrieve the (innermost) {@link ExecutionStageLoop} that this instance is part of.
     *
     * @return the said {@link ExecutionStageLoop} or {@code null} if none
     */
    public ExecutionStageLoop getLoop() {
        return this.executionStageLoop;
    }

    /**
     * Retrieves the {@link LoopHeadOperator} {@link ExecutionTask} in this instance. This instance must be a
     * loop head.
     *
     * @return the {@link LoopHeadOperator} {@link ExecutionTask}
     * @see #isLoopHead()
     */
    public ExecutionTask getLoopHeadTask() {
        assert this.isLoopHead();
        return RheemCollections.getSingle(this.getAllTasks());
    }

    /**
     * Tells whether this instance is in a {@link ExecutionStageLoop} that has finished iterating.
     *
     * @return whether this instance is in a finished {@link ExecutionStageLoop}
     * @see #isLoopHead()
     */
    public boolean isInFinishedLoop() {
        if (this.executionStageLoop == null) {
            return false;
        }
        final LoopHeadOperator loopHeadOperator = (LoopHeadOperator) this.executionStageLoop.getLoopHead().getLoopHeadTask().getOperator();
        return loopHeadOperator.getState() == LoopHeadOperator.State.FINISHED;
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
        return String.format("%s%s", this.getClass().getSimpleName(), this.getStartTasks());
    }

    @SuppressWarnings("unused")
    public String toNameString() {
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

    /**
     * Prints the instance's {@link ExecutionTask}s and {@link Channel}s.
     *
     * @return a {@link String} containing the textual representation
     */
    public String getPlanAsString() {
        return this.getPlanAsString("");
    }

    /**
     * Prints the instance's {@link ExecutionTask}s and {@link Channel}s.
     *
     * @param indent will be used to indent every line of the textual representation
     * @return a {@link String} containing the textual representation
     */
    public String getPlanAsString(String indent) {
        final StringBuilder sb = new StringBuilder();
        this.getPlanAsString(sb, indent);
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    /**
     * Appends this instance's {@link ExecutionTask}s and {@link Channel}s to the given {@link StringBuilder}.
     *
     * @param sb     to which the representation should be appended
     * @param indent will be used to indent every line of the textual representation
     */
    public void getPlanAsString(StringBuilder sb, String indent) {
        Set<ExecutionTask> seenTasks = new HashSet<>();
        for (ExecutionTask startTask : this.startTasks) {
            for (Channel inputChannel : startTask.getInputChannels()) {
                sb.append(indent)
                        .append("In  ")
                        .append(this.prettyPrint(inputChannel))
                        .append(" => ")
                        .append(this.prettyPrint(startTask)).append('\n');
            }
            this.toExtensiveStringAux(startTask, seenTasks, sb, indent);
        }
    }

    private void toExtensiveStringAux(ExecutionTask task, Set<ExecutionTask> seenTasks, StringBuilder sb, String indent) {
        if (!seenTasks.add(task)) {
            return;
        }
        for (Channel channel : task.getOutputChannels()) {
            for (ExecutionTask consumer : channel.getConsumers()) {
                if (consumer.getStage() == this) {
                    sb.append(indent)
                            .append("    ")
                            .append(this.prettyPrint(task))
                            .append(" => ")
                            .append(this.prettyPrint(channel))
                            .append(" => ")
                            .append(this.prettyPrint(consumer)).append('\n');
                    this.toExtensiveStringAux(consumer, seenTasks, sb, indent);
                } else {
                    sb.append(indent)
                            .append("Out ")
                            .append(this.prettyPrint(task))
                            .append(" => ")
                            .append(this.prettyPrint(channel)).append('\n');
                }
            }
        }
    }

    public Map toJsonMap() {
        HashMap<String, Object> jsonMap = new HashMap<>();
        ArrayList<Map> operators = new ArrayList<>();

        jsonMap.put("platform", this.getPlatformExecution().getPlatform().getName());
        jsonMap.put("operators", operators);
        Set<ExecutionTask> seenTasks = new HashSet<>();
        for (ExecutionTask startTask : this.startTasks) {
            this.toJsonMapAux(startTask, seenTasks, operators);
        }
        return  jsonMap;
    }

    private void toJsonMapAux(ExecutionTask task, Set<ExecutionTask> seenTasks, ArrayList operators) {
        if (!seenTasks.add(task)) {
            return;
        }
        HashMap operator = new HashMap();
        HashMap<String, ArrayList<HashMap<String, Object>>>  jsonConnectsTo = new HashMap<>();
        operator.put("name", task.getOperator().getName());
        operator.put("is_terminal", this.terminalTasks.contains(task) ? 1:0);
        operator.put("is_start", this.startTasks.contains(task) ? 1:0);
        operator.put("java_class", task.getOperator().getClass().getName());

        /*
            connects_to should look like this:
            "connects_to": {"0": [{"via": "CollectionChannel", "javaFlatMapOperator": 0}]}
         */
        operator.put("connects_to", jsonConnectsTo);
        operators.add(operator);

        for (Channel channel : task.getOutputChannels()) {
            ArrayList<HashMap<String, Object>> perOutputThatList = new ArrayList<>();
            Integer thisOutIndex = channel.getProducerSlot()==null ? 0 : channel.getProducerSlot().getIndex();
            jsonConnectsTo.put(thisOutIndex.toString(), perOutputThatList);

            for (ExecutionTask consumer : channel.getConsumers()) {
                HashMap<String, Object> jsonThatOp = new HashMap<>();
                jsonThatOp.put(consumer.getOperator().getName(),
                        (consumer.getInputSlotFor(channel)==null) ? 0 : consumer.getInputSlotFor(channel).getIndex());
                jsonThatOp.put("via", prettyPrint(channel));
                perOutputThatList.add(jsonThatOp);
                if (consumer.getStage() == this)
                    this.toJsonMapAux(consumer, seenTasks, operators);
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

}
