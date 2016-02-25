package org.qcri.rheem.core.optimizer.enumeration;

import org.qcri.rheem.core.plan.executionplan.*;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Creates an {@link PreliminaryExecutionPlan} from a {@link PartialPlan}.
 */
public class ExecutionPlanCreator extends AbstractTopologicalTraversal<Void,
        ExecutionPlanCreator.Activator,
        ExecutionPlanCreator.Activation> {

    private final Map<ExecutionOperator, Activator> activators = new HashMap<>();

    private final Collection<Activator> startActivators;

    private final Collection<Activation> startActivations;

    private final PartialPlan partialPlan;

    private final Collection<ExecutionTask> terminalTasks = new LinkedList<>();

    private final Map<ExecutionOperator, ExecutionTask> executionTasks = new HashMap<>();

    private final Set<Channel> inputChannels = new HashSet<>();

    /**
     * Creates a new instance that enumerates a <i>complete</i> {@link ExecutionPlan}.
     *
     * @param startOperators {@link ExecutionOperator}s from which the enumeration can start (should be sources).
     * @param partialPlan    defines the {@link ExecutionOperator}s to use
     */
    public ExecutionPlanCreator(Collection<ExecutionOperator> startOperators, PartialPlan partialPlan) {
        this.partialPlan = partialPlan;
        this.startActivators = startOperators.stream().map(Activator::new).collect(Collectors.toList());
        this.startActivations = Collections.emptyList();
    }

    /**
     * Creates a new instance that enumerates a <i>partial</i> {@link ExecutionPlan}. In fact, provides additional
     * {@link Channel}s that have already been processed, so all their producers must not be enumerated.
     *
     * @param startOperators {@link ExecutionOperator}s from which the enumeration can start (should be sources).
     * @param partialPlan    defines the {@link ExecutionOperator}s to use
     * @param existingPlan   {@link ExecutionPlan} that has already been executed and should be enhanced now; note that
     *                       it must agree with the {@code partialPlan}
     * @param openChannels   they, and their producers, must not be enumerated
     */
    public ExecutionPlanCreator(Collection<ExecutionOperator> startOperators,
                                PartialPlan partialPlan,
                                ExecutionPlan existingPlan,
                                Set<Channel> openChannels,
                                Set<ExecutionStage> executedStages) {
        this.partialPlan = partialPlan;

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
            OutputSlot<?> producerOutput = this.findRheemPlanOutputSlotFor(channel);

            // Now find all InputSlots that are fed by the OutputSlot and whose Operators have not yet been executed.
            Collection<InputSlot<?>> consumerInputs = this.findRheemPlanInputSlotFor(producerOutput);

            // Finally, produce Activations.
            if (!consumerInputs.isEmpty()) {
                Channel channelCopy = channel.copy();
                this.inputChannels.add(channelCopy);
                for (InputSlot<?> consumerInput : consumerInputs) {
                    this.logger.debug("Intercepting {}->{}.", producerOutput, consumerInput);
                    final ExecutionOperator consumerOperator = (ExecutionOperator) consumerInput.getOwner();
                    final Activator consumerActivator = this.activators.computeIfAbsent(consumerOperator, Activator::new);
                    final ExecutionTask consumerTask = this.getOrCreateExecutionTask(consumerOperator);
                    consumerActivator.executionTask = consumerTask;
                    final Platform consumerPlatform = consumerTask.getOperator().getPlatform();
                    final ChannelInitializer channelInitializer =
                            consumerPlatform.getChannelManager().getChannelInitializer(channelCopy.getDescriptor());
                    if (channelInitializer == null) {
                        throw new AbortException(String.format("Cannot connect %s to %s.", channel, consumerTask));
                    }
                    channelInitializer.setUpInput(channelCopy, consumerTask, consumerInput.getIndex());
                    this.startActivations.add(new Activation(consumerActivator, consumerInput.getIndex()));
                }
            }
        }
    }

    private Collection<InputSlot<?>> findRheemPlanInputSlotFor(OutputSlot<?> producerOutput) {
        return producerOutput.getOwner().getOutermostOutputSlots(producerOutput).stream()
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .flatMap(this::findExecutionOperatorInputs)
                .collect(Collectors.toList());
    }

    private Stream<InputSlot<?>> findExecutionOperatorInputs(InputSlot<?> input) {
        final Operator owner = input.getOwner();
        if (!owner.isAlternative()) {
            return Stream.of(input);
        }
        OperatorAlternative.Alternative alternative =
                ExecutionPlanCreator.this.partialPlan.getChosenAlternative((OperatorAlternative) owner);
        if (alternative == null) {
            ExecutionPlanCreator.this.logger.warn(
                    "Deciding upon output channels before having settled all follow-up alternatives.");
            return Stream.empty();
        }
        return alternative.followInput(input).stream().flatMap(this::findExecutionOperatorInputs);
    }


    /**
     * Determine the producing {@link OutputSlot} of this {@link Channel} that lies within a {@link RheemPlan}.
     * We follow non-RheemPlan {@link ExecutionOperator}s because they should merely forward data.
     */
    private OutputSlot<?> findRheemPlanOutputSlotFor(Channel openChannel) {
        OutputSlot<?> producerOutput = null;
        Channel tracedChannel = openChannel;
        do {
            final ExecutionTask producer = tracedChannel.getProducer();
            final ExecutionOperator producerOperator = producer.getOperator();
            if (this.checkIfRheemPlanOperator(producerOperator)) {
                producerOutput = producer.getOutputSlotFor(tracedChannel);
            } else {
                assert producer.getNumInputChannels() == 1;
                tracedChannel = producer.getInputChannel(0);
            }
        } while (producerOutput == null);
        return producerOutput;
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
            ;
            if (this.checkIfRheemPlanOperator(consumerTask.getOperator())) {
                result.add(consumerTask.getInputSlotFor(channel));
            } else {
                for (Channel consumerOutputChannel : consumerTask.getOutputChannels()) {
                    result.addAll(this.findRheemPlanInputSlotFor(consumerOutputChannel, executedStages));
                }
            }
        }
        return result;
    }

    /**
     * Heuristically determines if an {@link ExecutionOperator} was specified in a {@link RheemPlan} or if
     * it has been inserted by Rheem in a later stage.
     *
     * @param operator should be checked
     * @return whether the {@code operator} is deemed to be user-specified
     */
    private boolean checkIfRheemPlanOperator(ExecutionOperator operator) {
        // A non-RheemPlan operator is presumed to be "free floating" and completely unconnected. Connections are only
        // maintained via ExecutionTasks and Channels.
        return !(operator.getParent() == null
                && Arrays.stream(operator.getAllInputs())
                .map(InputSlot::getOccupant)
                .allMatch(Objects::isNull)
                && Arrays.stream(operator.getAllOutputs())
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .allMatch(Objects::isNull)
        );
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
     * Takes care of creating {@link ExecutionTask}s and {@link Channel}s.
     */
    public class Activator extends AbstractTopologicalTraversal.Activator<Activation> {

        private Activation[] activations;

        private ExecutionTask executionTask;

        public Activator(ExecutionOperator operator) {
            super(operator);
            this.activations = new Activation[operator.getNumInputs()];
        }

        @Override
        protected boolean isActivationComplete() {
            return Arrays.stream(this.activations).noneMatch(Objects::isNull);
        }

        @Override
        protected Collection<Activation> doWork() {
            this.executionTask = ExecutionPlanCreator.this.getOrCreateExecutionTask((ExecutionOperator) this.operator);
            final Platform platform = ((ExecutionOperator) this.operator).getPlatform();

            // Create a Channel for each OutputSlot of the wrapped Operator.
            Collection<Activation> collector = new LinkedList<>();
            for (int outputIndex = 0; outputIndex < this.operator.getNumOutputs(); outputIndex++) {
                if (!this.establishChannels(outputIndex, platform, collector)) return null;
            }

            // If we could not create any Activation, then we safe the current operator.
            if (collector.isEmpty()) {
                ExecutionPlanCreator.this.terminalTasks.add(this.executionTask);
            }

            return collector;
        }

        private boolean establishChannels(int outputIndex, Platform platform, Collection<Activation> collector) {
            // Collect all InputSlots that are connected to the current OutputSlot.
            final List<InputSlot<Object>> targetInputs = this.operator
                    .getOutermostOutputSlots(this.operator.getOutput(outputIndex).unchecked())
                    .stream()
                    .flatMap(output -> output.getOccupiedSlots().stream())
                    .flatMap(ExecutionPlanCreator.this::findExecutionOperatorInputs)
                    .map(InputSlot::unchecked)
                    .collect(Collectors.toList());

            // Create the activations already.
            for (InputSlot<Object> targetInput : targetInputs) {
                this.createActivation(targetInput, collector);
            }

            final List<Tuple<ExecutionTask, Integer>> targetExecutionTasks =
                    targetInputs.stream()
                            .map(input -> new Tuple<>(
                                    ExecutionPlanCreator.this.getOrCreateExecutionTask((ExecutionOperator) input.getOwner()),
                                    input.getIndex()
                            ))
                            .collect(Collectors.toList());


            // Create the connections.
            return platform.getChannelManager().connect(this.executionTask, outputIndex, targetExecutionTasks);
        }

        private void createActivation(InputSlot<Object> targetInput, Collection<Activation> collector) {
            final Operator targetOperator = targetInput.getOwner();
            if (targetOperator.isAlternative()) {
                OperatorAlternative.Alternative alternative =
                        ExecutionPlanCreator.this.partialPlan.getChosenAlternative((OperatorAlternative) targetOperator);
                if (alternative != null) {
                    final Collection<InputSlot<Object>> innerTargetInputs = alternative.followInput(targetInput);
                    for (InputSlot<Object> innerTargetInput : innerTargetInputs) {
                        this.createActivation(innerTargetInput, collector);
                    }
                }
            } else if (targetOperator.isExecutionOperator()) {
                final Activator activator =
                        ExecutionPlanCreator.this.activators.computeIfAbsent((ExecutionOperator) targetOperator, Activator::new);
                collector.add(new Activation(activator, targetInput.getIndex()));
            } else {
                throw new IllegalStateException("Unexpected operator: " + targetOperator);
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

    }
}
