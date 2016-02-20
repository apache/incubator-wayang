package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.util.Counter;

import java.util.*;
import java.util.stream.Collectors;

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

    /**
     * Scrap {@link Channel}s and {@link ExecutionTask}s that are not within the given {@link ExecutionStage}s.
     */
    public void retain(Set<ExecutionStage> retainableStages) {
        for (ExecutionStage stage : retainableStages) {
            for (ExecutionTask terminalTask : stage.getTerminalTasks()) {
                for (Channel channel : terminalTask.getOutputChannels()) {
                    channel.retain(retainableStages);
                }
            }
            stage.retainSuccessors(retainableStages);
            stage.getPlatformExecution().retain(retainableStages);
        }
    }

    /**
     * Collects all {@link ExecutionTask}s of this instance.
     *
     * @return the {@link ExecutionTask}s
     */
    public Set<ExecutionTask> collectAllTasks() {
        // TODO: Cache? What if we enhance this instance? Then we need to invalidate it.
        Set<ExecutionTask> allTasks = new HashSet<>();
        Set<ExecutionStage> seenStages = new HashSet<>();
        Queue<ExecutionStage> openStages = new LinkedList<>(this.getStartingStages());
        while (!openStages.isEmpty()) {
            final ExecutionStage stage = openStages.poll();
            if (seenStages.add(stage)) {
                allTasks.addAll(stage.getAllTasks());
                openStages.addAll(stage.getSuccessors());
            }
        }
        return allTasks;
    }

    /**
     * The given instance should build upon the open {@link Channel}s of this instance. Then, this instance will be
     * expanded with the content of the given instance.
     *
     * @param expansion extends this instance, but they are not overlapping
     */
    public void expand(ExecutionPlan expansion) {
        for (Channel openChannel : expansion.getOpenInputChannels()) {
            openChannel.mergeIntoOriginal();
            final Channel original = openChannel.getOriginal();
            final ExecutionStage producerStage = original.getProducer().getStage();
            for (ExecutionTask consumer : original.getConsumers()) {
                final ExecutionStage consumerStage = consumer.getStage();
                assert producerStage != null : String.format("No stage found for %s.", producerStage);
                producerStage.addSuccessor(consumerStage);
            }
        }
    }

    /**
     * Collects all {@link Channel}s that are copies. We recognize them as {@link Channel}s that are open.
     */
    public Collection<Channel> getOpenInputChannels() {
        return this.collectAllTasks().stream()
                .flatMap(task -> Arrays.stream(task.getInputChannels()))
                .filter(Channel::isCopy)
                .collect(Collectors.toList());
    }

}
