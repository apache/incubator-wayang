package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorAlternative;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates an {@link ExecutionTaskFlow} from a {@link PlanImplementation}.
 */
public class ExecutionTaskFlowCompiler
        extends AbstractTopologicalTraversal<ExecutionTaskFlowCompiler.Activator, ExecutionTaskFlowCompiler.Activation> {

    /**
     * Maintains {@link Activator}s.
     */
    private final Map<ActivatorKey, Activator> activators = new HashMap<>();

    /**
     * Initial {@link Activator}s.
     */
    private final Collection<Activator> startActivators;

    /**
     * Initial {@link Activation}s.
     */
    private final Collection<Activation> startActivations;

    /**
     * The top-level {@link PlanImplementation} that should be converted into an {@link ExecutionTaskFlow}.
     */
    private final PlanImplementation planImplementation;

    private final Collection<ExecutionTask> terminalTasks = new LinkedList<>();

    private final Map<ExecutionOperator, ExecutionTask> executionTasks = new HashMap<>();

    private final Set<Channel> inputChannels = new HashSet<>();

    /**
     * Creates a new instance that enumerates a <i>complete</i> {@link ExecutionPlan}.
     *
     * @param startOperators     {@link ExecutionOperator}s from which the enumeration can start (should be sources).
     * @param planImplementation defines the {@link ExecutionOperator}s to use
     */
    public ExecutionTaskFlowCompiler(Collection<ExecutionOperator> startOperators, PlanImplementation planImplementation) {
        this.planImplementation = planImplementation;
        this.startActivators = startOperators.stream().map(Activator::new).collect(Collectors.toList());
        this.startActivations = Collections.emptyList();
    }

    /**
     * Creates a new instance that enumerates a <i>partial</i> {@link ExecutionPlan}. In fact, provides additional
     * {@link Channel}s that have already been processed, so all their producers must not be enumerated.
     *
     * @param startOperators     {@link ExecutionOperator}s from which the enumeration can start (should be sources).
     * @param planImplementation defines the {@link ExecutionOperator}s to use
     * @param existingPlan       {@link ExecutionPlan} that has already been executed and should be enhanced now; note that
     *                           it must agree with the {@code planImplementation}
     * @param openChannels       they, and their producers, must not be enumerated
     */
    public ExecutionTaskFlowCompiler(Collection<ExecutionOperator> startOperators,
                                     PlanImplementation planImplementation,
                                     ExecutionPlan existingPlan,
                                     Set<Channel> openChannels,
                                     Set<ExecutionStage> executedStages) {
        this.planImplementation = planImplementation;

        // We use the following reasoning to determine where to start the traversal:
        // Premise: start Operator is involved in producing an existing Channel <=> Operator has been executed
        // So, we need to keep start Operators that are not (indirectly) producing a Channel and the Channels themselves

        // Create Activators for the unexecuted start Operators.
        final Set<ExecutionOperator> executedOperators = existingPlan.collectAllTasks().stream()
                .map(ExecutionTask::getOperator)
                .collect(Collectors.toSet());
        this.startActivators = startOperators.stream()
                .filter(operator -> !executedOperators.contains(operator))
                .map(Activator::new)
                .collect(Collectors.toList());

        // Create Activations for the open Channels.
        this.startActivations = new LinkedList<>();
        for (Channel channel : openChannels) {
            // Detect the Slot connections that have yet to be fulfilled by this Channel.
            OutputSlot<?> producerOutput = OptimizationUtils.findRheemPlanOutputSlotFor(channel);
            assert producerOutput != null : String.format("No producing output for %s.", channel);

            final LoopImplementation.IterationImplementation producerIterationImplementation =
                    this.findIterationImplementation(producerOutput);
            final PlanImplementation producerPlanImplementation = producerIterationImplementation == null ?
                    this.planImplementation :
                    producerIterationImplementation.getBodyImplementation();
            final Junction openJunction = producerPlanImplementation.getJunction(producerOutput);
            assert openJunction != null : String.format("No junction for %s.", producerOutput);

            for (int targetIndex = 0; targetIndex < openJunction.getNumTargets(); targetIndex++) {
                final InputSlot<?> targetInput = openJunction.getTargetInput(targetIndex);
                final ExecutionOperator consumerOperator = (ExecutionOperator) targetInput.getOwner();

                // If the channel was only "partially open", then we need to consider not to re-create existing ExecutionTasks.
                if (executedOperators.contains(consumerOperator)) continue;

                final LoopImplementation.IterationImplementation consumerIterationImplementation =
                        this.findIterationImplementation(targetInput);
                final ActivatorKey activatorKey = new ActivatorKey(consumerOperator, consumerIterationImplementation);
                final Activator consumerActivator = this.activators.computeIfAbsent(activatorKey, Activator::new);
                final ExecutionTask consumerTask = this.getOrCreateExecutionTask(consumerOperator);
                consumerActivator.executionTask = consumerTask;
                final Channel targetChannel = openJunction.getTargetChannel(targetIndex);
                targetChannel.addConsumer(consumerTask, targetInput.getIndex());
                this.startActivations.add(new Activation(consumerActivator, targetInput.getIndex()));

            }

//            // Now find all InputSlots that are fed by the OutputSlot and whose Operators have not yet been executed.
//            Collection<InputSlot<?>> consumerInputs = this.findRheemPlanInputSlotFor(producerOutput);
//
//            // Finally, produce Activations.
//            if (!consumerInputs.isEmpty()) {
//                Channel channelCopy = channel.copy();
//                this.inputChannels.add(channelCopy);
//                // If the channel was only "partially open", then we need to consider not to re-create existing ExecutionTasks.
//                final Set<InputSlot<?>> connectedInputSlots = channel.getConsumers().stream()
//                        .map(consumer -> consumer.getInputSlotFor(channel))
//                        .collect(Collectors.toSet());
//                for (InputSlot<?> consumerInput : consumerInputs) {
//                    if (connectedInputSlots.contains(consumerInput)) {
//                        this.logger.debug("Not creating ExecutionTasks for {}.", consumerInput);
//                        continue;
//                    }
//                    this.logger.debug("Intercepting {}->{}.", producerOutput, consumerInput);
//                    final ExecutionOperator consumerOperator = (ExecutionOperator) consumerInput.getOwner();
//                    final ActivatorKey activatorKey = new ActivatorKey(consumerOperator, null);
//                    final Activator consumerActivator = this.activators.computeIfAbsent(activatorKey, Activator::new);
//                    final ExecutionTask consumerTask = this.getOrCreateExecutionTask(consumerOperator);
//                    consumerActivator.executionTask = consumerTask;
////                    final Platform consumerPlatform = consumerTask.getOperator().getPlatform();
////                    final ChannelInitializer channelInitializer =
////                            consumerPlatform.getChannelManager().getChannelInitializer(channelCopy.getDescriptor());
////                    if (channelInitializer == null) {
////                        throw new AbortException(String.format("Cannot connect %s to %s.", channel, consumerTask));
////                    }
//                    // Is this correct?
//                    channelCopy.addConsumer(consumerTask, consumerInput.getIndex());
//                    // todo: rewrite the whole thing
////                    channelInitializer.setUpInput(channelCopy, consumerTask, consumerInput.getIndex());
//                    this.startActivations.add(new Activation(consumerActivator, consumerInput.getIndex()));
//                }
//            }
        }
    }

    private Collection<InputSlot<?>> findRheemPlanInputSlotFor(OutputSlot<?> producerOutput) {
        return producerOutput.getOwner().getOutermostOutputSlots(producerOutput).stream()
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .flatMap(this::findExecutionOperatorInputs)
                .collect(Collectors.toList());
    }

    /**
     * Find the {@link LoopImplementation.IterationImplementation} of an {@link ExecutionOperator}'s {@link OutputSlot}.
     *
     * @param output the {@link OutputSlot}
     * @return the {@link LoopImplementation.IterationImplementation} or {@code null} if the {@link OutputSlot}
     * is not inside a {@link LoopSubplan}
     */
    private LoopImplementation.IterationImplementation findIterationImplementation(OutputSlot<?> output) {
        PlanImplementation planImplementation = this.planImplementation;
        if (this.planImplementation.getJunction(output) != null) return null;
        LoopImplementation.IterationImplementation iterationImplementation = null;

        // Descend into the nested PlanImplementations according to the loop stack of the operator.
        final ExecutionOperator operator = (ExecutionOperator) output.getOwner();
        final LinkedList<LoopSubplan> loopStack = operator.getLoopStack();
        for (LoopSubplan loop : loopStack) {
            LoopImplementation loopImplementation = planImplementation.getLoopImplementations().get(loop);
            iterationImplementation = loopImplementation.getSingleIterationImplementation();
            planImplementation = iterationImplementation.getBodyImplementation();
            if (planImplementation.getJunction(output) != null) break;
        }

        return iterationImplementation;
    }

    /**
     * Find the {@link LoopImplementation.IterationImplementation} of an {@link ExecutionOperator}'s {@link InputSlot}.
     *
     * @param input the {@link InputSlot}
     * @return the {@link LoopImplementation.IterationImplementation} or {@code null} if the {@link InputSlot}
     * is not inside a {@link LoopSubplan}
     */
    private LoopImplementation.IterationImplementation findIterationImplementation(InputSlot<?> input) {
        final ExecutionOperator operator = (ExecutionOperator) input.getOwner();
        PlanImplementation planImplementation = this.planImplementation;
        LoopImplementation.IterationImplementation iterationImplementation = null;

        // Descend into the nested PlanImplementations according to the loop stack of the operator.
        final LinkedList<LoopSubplan> loopStack = operator.getLoopStack();
        for (LoopSubplan loop : loopStack) {
            LoopImplementation loopImplementation = planImplementation.getLoopImplementations().get(loop);
            iterationImplementation = loopImplementation.getSingleIterationImplementation();
            planImplementation = iterationImplementation.getBodyImplementation();
        }

        return iterationImplementation;
    }

    private Stream<InputSlot<?>> findExecutionOperatorInputs(InputSlot<?> input) {
        final Operator owner = input.getOwner();
        if (!owner.isAlternative()) {
            return Stream.of(input);
        }
        OperatorAlternative.Alternative alternative =
                ExecutionTaskFlowCompiler.this.planImplementation.getChosenAlternative((OperatorAlternative) owner);
        if (alternative == null) {
            ExecutionTaskFlowCompiler.this.logger.warn(
                    "Deciding upon output channels before having settled all follow-up alternatives.");
            return Stream.empty();
        }
        return alternative.followInput(input).stream().flatMap(this::findExecutionOperatorInputs);
    }


    /**
     * Determine the consuming {@link InputSlot}s of the given {@link Channel} that lie within a {@link RheemPlan} and
     * have not been executed yet.
     * We follow non-RheemPlan {@link ExecutionOperator}s because they should merely forward data.
     */
    private Collection<InputSlot<?>> findRheemPlanInputSlotFor(Channel channel, Set<ExecutionStage> executedStages) {
        Collection<InputSlot<?>> result = new LinkedList<>();
        for (ExecutionTask consumerTask : channel.getConsumers()) {
            if (executedStages.contains(consumerTask.getStage())) continue;
            if (!consumerTask.getOperator().isAuxiliary()) {
                result.add(consumerTask.getInputSlotFor(channel));
            } else {
                for (Channel consumerOutputChannel : consumerTask.getOutputChannels()) {
                    result.addAll(this.findRheemPlanInputSlotFor(consumerOutputChannel, executedStages));
                }
            }
        }
        return result;
    }

    private ExecutionTask getOrCreateExecutionTask(ExecutionOperator executionOperator) {
        return this.executionTasks.computeIfAbsent(executionOperator, ExecutionTask::new);
    }

    @Override
    protected Collection<Activator> getInitialActivators() {
        return this.startActivators;
    }

    @Override
    protected Collection<Activation> getInitialActivations(int index) {
        return this.startActivations;
    }

    @Override
    protected int getNumInitialActivations() {
        return 1;
    }

    public Collection<ExecutionTask> getTerminalTasks() {
        return this.terminalTasks;
    }

    public Set<Channel> getInputChannels() {
        return this.inputChannels;
    }

    /**
     * Identifies an {@link Activator}.
     */
    private static final class ActivatorKey {

        private final ExecutionOperator executionOperator;

        private final LoopImplementation.IterationImplementation iterationImplementation;

        private ActivatorKey(ExecutionOperator executionOperator,
                             LoopImplementation.IterationImplementation iterationImplementation) {
            this.executionOperator = executionOperator;
            this.iterationImplementation = iterationImplementation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;
            ActivatorKey that = (ActivatorKey) o;
            return Objects.equals(this.executionOperator, that.executionOperator) &&
                    Objects.equals(this.iterationImplementation, that.iterationImplementation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.executionOperator, this.iterationImplementation);
        }
    }

    /**
     * Takes care of creating {@link ExecutionTask}s and {@link Channel}s.
     */
    public class Activator extends AbstractTopologicalTraversal.Activator<Activation> {

        private final Activation[] activations;

        private ExecutionTask executionTask;

        /**
         * The {@link LoopImplementation.IterationImplementation} in which the {@link #operator} resides or
         * {@code null} if none.
         */
        private final LoopImplementation.IterationImplementation iterationImplementation;

        /**
         * Convenience constructor for when we are not inside of a {@link LoopImplementation.IterationImplementation}.
         *
         * @param operator
         */
        public Activator(ExecutionOperator operator) {
            this(operator, null);
        }

        /**
         * Convenience constructor.
         *
         * @param key identifies the new instance
         */
        public Activator(ActivatorKey key) {
            this(key.executionOperator, key.iterationImplementation);
        }

        /**
         * Creates a new instance.
         *
         * @param operator                that should be processed
         * @param iterationImplementation in which the {@code operator} resides (or {@code null} if none)
         */
        public Activator(ExecutionOperator operator, LoopImplementation.IterationImplementation iterationImplementation) {
            super(operator);
            this.activations = new Activation[operator.getNumInputs()];
            this.iterationImplementation = iterationImplementation;
        }

        @Override
        protected boolean isActivationComplete() {
            assert this.activations.length == this.operator.getNumInputs();
            for (int inputIndex = 0; inputIndex < this.operator.getNumInputs(); inputIndex++) {
                if (this.activations[inputIndex] == null && !this.operator.getInput(inputIndex).isFeedback()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        protected Collection<Activation> doWork() {
            this.executionTask = ExecutionTaskFlowCompiler.this.getOrCreateExecutionTask((ExecutionOperator) this.operator);
            final Platform platform = ((ExecutionOperator) this.operator).getPlatform();

            // Create a Channel for each OutputSlot of the wrapped Operator.
            Collection<Activation> collector = new LinkedList<>();
            for (int outputIndex = 0; outputIndex < this.operator.getNumOutputs(); outputIndex++) {
                this.connectToSuccessorTasks(outputIndex, platform, collector);
            }

            // If we could not create any Activation, then we safe the current operator.
            if (collector.isEmpty()) {
                ExecutionTaskFlowCompiler.this.terminalTasks.add(this.executionTask);
            }

            return collector;
        }

        private void connectToSuccessorTasks(int outputIndex, Platform platform, Collection<Activation> collector) {
            final OutputSlot<?> output = this.operator.getOutput(outputIndex);
            // TODO: Make generic: There might be multiple OutputSlots for final loop outputs (one for each iteration).
            final Junction junction = this.getJunction(output);
            LoggerFactory.getLogger(this.getClass()).debug("Connecting {} -> {}.", output, junction);
            assert junction != null : String.format("No junction found for %s.", output);
            this.executionTask.setOutputChannel(outputIndex, junction.getSourceChannel());

            for (int targetIndex = 0; targetIndex < junction.getNumTargets(); targetIndex++) {
                final Channel targetChannel = junction.getTargetChannel(targetIndex);
                final InputSlot<?> targetInput = junction.getTargetInput(targetIndex);
                final ExecutionTask successorTask =
                        ExecutionTaskFlowCompiler.this.getOrCreateExecutionTask((ExecutionOperator) targetInput.getOwner());
                targetChannel.addConsumer(successorTask, targetInput.getIndex());

                this.createActivation(targetInput.unchecked(), collector);
            }
        }

        private Junction getJunction(OutputSlot<?> output) {
            if (this.iterationImplementation != null) {
                final Junction junction = this.iterationImplementation.getBodyImplementation().getJunction(output);
                if (junction != null) return junction;
            }
            return ExecutionTaskFlowCompiler.this.planImplementation.getJunction(output);
        }

        /**
         * Creates an {@link Activator} for the {@link ExecutionOperator} idenfied by the {@code targetInput} and
         * adds an {@link Activation} to it.
         */
        private void createActivation(InputSlot<Object> targetInput, Collection<Activation> collector) {
            final Operator targetOperator = targetInput.getOwner();
            if (targetOperator.isAlternative()) {
                OperatorAlternative.Alternative alternative =
                        ExecutionTaskFlowCompiler.this.planImplementation.getChosenAlternative((OperatorAlternative) targetOperator);
                if (alternative == null) {
                    throw new IllegalStateException("No selected alternative for " + targetOperator);
                }
                final Collection<InputSlot<Object>> innerTargetInputs = alternative.followInput(targetInput);
                for (InputSlot<Object> innerTargetInput : innerTargetInputs) {
                    this.createActivation(innerTargetInput, collector);
                }
            } else if (targetOperator.isExecutionOperator()) {
                for (final LoopImplementation.IterationImplementation targetIteration : this.determineIteration(targetOperator)) {
                    final ActivatorKey activatorKey = new ActivatorKey((ExecutionOperator) targetOperator, targetIteration);
                    final Activator activator =
                            ExecutionTaskFlowCompiler.this.activators.computeIfAbsent(activatorKey, Activator::new);
                    collector.add(new Activation(activator, targetInput.getIndex()));
                }
            } else {
                throw new IllegalStateException("Unexpected operator: " + targetOperator);
            }
        }

        /**
         * As we enumerate {@link LoopSubplan}s, we might face multiple implementations. Here, we determine the
         * next {@link LoopImplementation.IterationImplementation} for the given {@code targetOperator} based on the
         * current {@link #iterationImplementation}.
         *
         * @param targetOperator for which the {@link LoopImplementation.IterationImplementation} is sought
         * @return the appropriate {@link LoopImplementation.IterationImplementation} (or {@code null} if n/a)
         */
        private Collection<LoopImplementation.IterationImplementation> determineIteration(Operator targetOperator) {
            // See if the targetOperator is inside a LoopSubplan in the first place.
            final LoopSubplan targetLoop = targetOperator.getInnermostLoop();
            if (targetLoop == null) return Collections.singleton(null);

            // Check if the targetOperator's loop has just been entered.
            final LoopSubplan currentLoop = this.operator.getInnermostLoop();
            if (currentLoop == null) { // TODO: Current code supports only non-nested loops.
                final LoopImplementation loopImplementation =
                        ExecutionTaskFlowCompiler.this.planImplementation.getLoopImplementations().get(targetLoop);
                if (targetOperator.isLoopHead()) {
                    return Collections.singleton(loopImplementation.getSingleIterationImplementation());
                } else {
                    return loopImplementation.getIterationImplementations().stream()
                            .filter(iterImpl -> true)
                            .collect(Collectors.toList());
                }
            }

            // Check if we are exiting a loop.
            // TODO: Current code (implicitly) supports only non-nested loops.

            // Otherwise, we are staying within a loop.
            assert currentLoop == targetLoop;
            // Check if we need to switch iterations.
            if (targetOperator.isLoopHead()) {
                return Collections.singleton(this.iterationImplementation.getSuccessorIterationImplementation());
            } else {
                return Collections.singleton(this.iterationImplementation);
            }


        }

        @Override
        protected void accept(Activation activation) {
            assert this.activations[activation.inputIndex] == null;
            this.activations[activation.inputIndex] = activation;
        }

    }

    /**
     * Propagates a {@link Channel} to its consumers.
     */
    public static class Activation extends AbstractTopologicalTraversal.Activation<Activator> {

        private final int inputIndex;

        protected Activation(Activator targetActivator, int inputIndex) {
            super(targetActivator);
            this.inputIndex = inputIndex;
        }

        /**
         * @return the {@link InputSlot} that is used to activate the following {@link Activator}
         */
        protected InputSlot<?> getActivatedInput() {
            return this.getTargetActivator().executionTask.getOperator().getInput(this.inputIndex);
        }

    }
}
