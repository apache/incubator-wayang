package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OperatorAlternative;
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

    private final Map<Operator, Activator> activators = new HashMap<>();

    private final Collection<Activator> startActivators;

    private final PartialPlan partialPlan;

    private final Collection<ExecutionTask> terminalTasks = new LinkedList<>();

    public ExecutionPlanCreator(Collection<ExecutionOperator> startOperators, PartialPlan partialPlan) {
        this.partialPlan = partialPlan;
        this.startActivators = startOperators.stream().map(Activator::new).collect(Collectors.toList());
    }

    @Override
    protected Collection<Activator> getInitialActivators() {
        return this.startActivators;
    }

    @Override
    protected Collection<Activation> getInitialActivations(int index) {
        return Collections.emptyList();
    }

    public Collection<ExecutionTask> getTerminalTasks() {
        return this.terminalTasks;
    }

    /**
     * Takes care of creating {@link ExecutionTask}s and {@link Channel}s.
     */
    public class Activator extends AbstractTopologicalTraversal.Activator<Activation> {

        private Activation[] activations;

        private ExecutionTask executionTask;

        public Activator(Operator operator) {
            super(operator);
            this.activations = new Activation[operator.getNumInputs()];
        }

        @Override
        protected boolean isActivationComplete() {
            return Arrays.stream(this.activations).noneMatch(Objects::isNull);
        }

        @Override
        protected Collection<Activation> doWork() {
            this.executionTask = new ExecutionTask((ExecutionOperator) this.operator);
            final Platform platform = ((ExecutionOperator) this.operator).getPlatform();

            for (int inputIndex = 0; inputIndex < this.activations.length; inputIndex++) {
                this.establishInputChannel(inputIndex, platform);
            }

            // Create a Channel for each OutputSlot of the wrapped Operator.
            Collection<Activation> collector = new LinkedList<>();
            for (int outputIndex = 0; outputIndex < this.operator.getNumOutputs(); outputIndex++) {
                if (this.establishOutputChannel(outputIndex, platform, collector)) return null;
            }

            // If we could not create any Activation, then we safe the current operator.
            if (collector.isEmpty()) {
                ExecutionPlanCreator.this.terminalTasks.add(this.executionTask);
            }

            return collector;
        }

        private void establishInputChannel(int inputIndex, Platform platform) {
            final Activation activation = this.activations[inputIndex];
            final ChannelInitializer<Channel> channelInitializer = platform
                    .getChannelInitializer(activation.channel.getClass())
                    .unchecked();
            if (channelInitializer == null) {
                throw new IllegalStateException(String.format("Inappropriate input %s selected for %s.",
                        activation.channel, this.operator));
            }
            channelInitializer.setUpInput(activation.channel, this.executionTask, inputIndex);
        }

        private boolean establishOutputChannel(int outputIndex, Platform platform, Collection<Activation> collector) {
            // Collect all InputSlots that are connectected to the current OutputSlot.
            final List<InputSlot<Object>> fedInputs =
                    this.operator.getOutermostOutputSlots(this.operator.getOutput(outputIndex).unchecked()).stream()
                            .flatMap(output -> output.getOccupiedSlots().stream())
                            .flatMap(input -> {
                                final Operator owner = input.getOwner();
                                if (!owner.isAlternative()) {
                                    return Stream.of(input);
                                }
                                OperatorAlternative.Alternative alternative =
                                        ExecutionPlanCreator.this.partialPlan.getChosenAlternative((OperatorAlternative) owner);
                                if (alternative == null) {
                                    ExecutionPlanCreator.this.logger.warn(
                                            "Deciding upon output channels for {} before having settled all follow-up alternatives.",
                                            this.operator);
                                    return Stream.empty();
                                }
                                return alternative.followInput(input).stream();
                            })
                            .collect(Collectors.toList());

            final List<InputSlot<Object>> internalInputs =
                    fedInputs.stream()
                            .filter(input -> ((ExecutionOperator) input.getOwner()).getPlatform().equals(platform))
                            .collect(Collectors.toList());

            final List<InputSlot<Object>> externalInputs =
                    fedInputs.stream()
                            .filter(input -> !((ExecutionOperator) input.getOwner()).getPlatform().equals(platform))
                            .collect(Collectors.toList());

            final Tuple<Class<? extends Channel>[], Class<? extends Channel>[]> pickedChannelClassTuple =
                    platform.pickChannelClasses((ExecutionOperator) this.operator, outputIndex, internalInputs, externalInputs);
            if (pickedChannelClassTuple == null) {
                ExecutionPlanCreator.this.logger.warn(
                        "Could not find an appropriate channel between {} and {}.", this.operator,
                        fedInputs.stream().map(InputSlot::getOwner).collect(Collectors.toList()));
                return true;
            }
            final Class<? extends Channel>[] internalClasses = pickedChannelClassTuple.field0;
            final Class<? extends Channel>[] externalClasses = pickedChannelClassTuple.field1;

            for (int i = 0; i < internalClasses.length; i++) {
                Class<? extends Channel> internalClass = internalClasses[i];
                final InputSlot<Object> input = internalInputs.get(i);
                final ChannelInitializer<? extends Channel> channelInitializer = platform.getChannelInitializer(internalClass);
                final Channel channel = channelInitializer.setUpOutput(this.executionTask, outputIndex);
                this.createActivation(channel, input, collector);
            }

            for (int i = 0; i < externalClasses.length; i++) {
                Class<? extends Channel> externalClass = externalClasses[i];
                final InputSlot<Object> input = externalInputs.get(i);
                final ChannelInitializer<? extends Channel> channelInitializer = platform.getChannelInitializer(externalClass);
                final Channel channel = channelInitializer.setUpOutput(this.executionTask, outputIndex);
                this.createActivation(channel, input, collector);
            }
            return false;
        }

        private void createActivation(Channel channel, InputSlot<Object> targetInput, Collection<Activation> collector) {
            final Operator targetOperator = targetInput.getOwner();
            if (targetOperator.isAlternative()) {
                OperatorAlternative.Alternative alternative =
                        ExecutionPlanCreator.this.partialPlan.getChosenAlternative((OperatorAlternative) targetOperator);
                if (alternative != null) {
                    final Collection<InputSlot<Object>> innerTargetInputs = alternative.followInput(targetInput);
                    for (InputSlot<Object> innerTargetInput : innerTargetInputs) {
                        this.createActivation(channel, innerTargetInput, collector);
                    }
                }
            } else if (targetOperator.isExecutionOperator()) {
                final Activator activator =
                        ExecutionPlanCreator.this.activators.computeIfAbsent(targetOperator, Activator::new);
                collector.add(new Activation(activator, channel, targetInput.getIndex()));
            } else {
                throw new IllegalStateException("Unexpected operator: " + targetOperator);
            }
        }

        @Override
        protected void accept(Activation activation) {
            Validate.isTrue(this.activations[activation.inputIndex] == null);
            this.activations[activation.inputIndex] = activation;
        }

    }

    /**
     * Propagates a {@link Channel} to its consumers.
     */
    public static class Activation extends AbstractTopologicalTraversal.Activation<Activator> {

        private final Channel channel;

        private final int inputIndex;

        protected Activation(Activator targetActivator, Channel channel, int inputIndex) {
            super(targetActivator);
            this.channel = channel;
            this.inputIndex = inputIndex;
        }

    }
}
