package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.optimizer.enumeration.ExecutionTaskFlow;
import org.qcri.rheem.core.optimizer.enumeration.StageAssignmentTraversal;
import org.qcri.rheem.core.util.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents an executable, cross-platform data flow. Consists of muliple {@link PlatformExecution}s.
 */
public class ExecutionPlan {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

    /**
     * Creates a {@link String} representation (not strictly ordered) of this instance.
     *
     * @return the {@link String} representation
     */
    public String toExtensiveString() {
        return this.toExtensiveString(false);
    }

    /**
     * Creates a {@link String} representation of this instance.
     *
     * @param isStriclyOrdering whether {@link ExecutionStage}s should be listed only after <i>all</i> their predecessors
     * @return the {@link String} representation
     */
    public String toExtensiveString(boolean isStriclyOrdering) {
        StringBuilder sb = new StringBuilder();
        Counter<ExecutionStage> stageActivationCounter = new Counter<>();
        Queue<ExecutionStage> activatedStages = new LinkedList<>(this.startingStages);
        Set<ExecutionStage> seenStages = new HashSet<>();
        while (!activatedStages.isEmpty()) {
            while (!activatedStages.isEmpty()) {
                final ExecutionStage stage = activatedStages.poll();
                if (!seenStages.add(stage)) continue;
                sb.append(">>> ").append(stage).append(":\n");
                stage.getPlanAsString(sb, "> ");
                sb.append("\n");

                for (ExecutionStage successor : stage.getSuccessors()) {
                    final int count = stageActivationCounter.add(successor, 1);
                    if (!isStriclyOrdering || count == successor.getPredecessors().size() || successor.isLoopHead()) {
                        activatedStages.add(successor);
                    }
                }
            }
        }
        return sb.toString();
    }

    public List<Map> toJsonList() {
        Counter<ExecutionStage> stageActivationCounter = new Counter<>();
        Queue<ExecutionStage> activatedStages = new LinkedList<>(this.startingStages);
        Set<ExecutionStage> seenStages = new HashSet<>();
        ArrayList<Map> allStages = new ArrayList<>();

        while (!activatedStages.isEmpty()) {
            final ExecutionStage stage = activatedStages.poll();
            if (!seenStages.add(stage)) continue;
            Map stageMap = stage.toJsonMap();

            // Better way to put sequence number ?
            stageMap.put("sequence_number", allStages.size());
            allStages.add(stageMap);
            for (ExecutionStage successor : stage.getSuccessors()) {
                final int count = stageActivationCounter.add(successor, 1);
                if (count == successor.getPredecessors().size() || successor.isLoopHead()) {
                    activatedStages.add(successor);
                }
            }
        }

        return allStages;
    }

    /**
     * Scrap {@link Channel}s and {@link ExecutionTask}s that are not within the given {@link ExecutionStage}s.
     *
     * @return {@link Channel}s from that consumer {@link ExecutionTask}s have been removed
     */
    public Set<Channel> retain(Set<ExecutionStage> retainableStages) {
        Set<Channel> openChannels = new HashSet<>();
        for (ExecutionStage stage : retainableStages) {
            for (Channel channel : stage.getOutboundChannels()) {
                if (channel.retain(retainableStages)) {
                    openChannels.add(channel);
                }
            }
            stage.retainSuccessors(retainableStages);
            stage.getPlatformExecution().retain(retainableStages);
        }
        return openChannels;
    }

    /**
     * Collects all {@link ExecutionStage}s in this instance.
     *
     * @return the {@link ExecutionStage}s
     */
    public Set<ExecutionStage> getStages() {
        Set<ExecutionStage> seenStages = new HashSet<>();
        Queue<ExecutionStage> openStages = new LinkedList<>(this.getStartingStages());
        while (!openStages.isEmpty()) {
            final ExecutionStage stage = openStages.poll();
            if (seenStages.add(stage)) {
                openStages.addAll(stage.getSuccessors());
            }
        }
        return seenStages;
    }

    /**
     * Collects all {@link ExecutionTask}s of this instance.
     *
     * @return the {@link ExecutionTask}s
     */
    public Set<ExecutionTask> collectAllTasks() {
        Set<ExecutionTask> allTasks = new HashSet<>();
        for (ExecutionStage stage : this.getStages()) {
            allTasks.addAll(stage.getAllTasks());
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
            assert producerStage != null : String.format("No stage found for %s.", original.getProducer());
            for (ExecutionTask consumer : original.getConsumers()) {
                final ExecutionStage consumerStage = consumer.getStage();
                assert consumerStage != null : String.format("No stage found for %s.", consumer);
                // Equal stages possible on "partially open" Channels.
                if (producerStage != consumerStage) {
                    producerStage.addSuccessor(consumerStage);
                }
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

    /**
     * Implements various sanity checks. Problems are logged.
     */
    public boolean isSane() {
        // 1. Check if every ExecutionTask is assigned an ExecutionStage.
        final Set<ExecutionTask> allTasks = this.collectAllTasks();
        boolean isAllTasksAssigned = allTasks.stream().allMatch(task -> task.getStage() != null);
        if (!isAllTasksAssigned) {
            this.logger.error("There are tasks without stages.");
        }

        final Set<Channel> allChannels = allTasks.stream()
                .flatMap(task -> Stream.concat(Arrays.stream(task.getInputChannels()), Arrays.stream(task.getOutputChannels())))
                .collect(Collectors.toSet());

        boolean isAllChannelsOriginal = allChannels.stream()
                .allMatch(channel -> !channel.isCopy());
        if (!isAllChannelsOriginal) {
            this.logger.error("There are channels that are copies.");
        }

        boolean isAllSiblingsConsistent = true;
        for (Channel channel : allChannels) {
            for (Channel sibling : channel.getSiblings()) {
                if (!allChannels.contains(sibling)) {
                    this.logger.error("A sibling of {}, namely {}, seems to be invalid.", channel, sibling);
                    isAllSiblingsConsistent = false;
                }
            }
        }

        return isAllTasksAssigned && isAllChannelsOriginal && isAllSiblingsConsistent;
    }

    /**
     * Creates a new instance from the given {@link ExecutionTaskFlow}.
     *
     * @param executionTaskFlow       should be converted into an {@link ExecutionPlan}
     * @param stageSplittingCriterion defines where to install {@link ExecutionStage} boundaries
     * @return the new instance
     */
    public static ExecutionPlan createFrom(ExecutionTaskFlow executionTaskFlow,
                                           StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion) {
        return StageAssignmentTraversal.assignStages(executionTaskFlow, stageSplittingCriterion);
    }

}
