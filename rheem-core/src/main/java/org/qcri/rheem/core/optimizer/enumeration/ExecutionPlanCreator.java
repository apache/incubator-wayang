package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;

import java.util.*;
import java.util.stream.Collectors;

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

    public boolean connect(ExecutionTask task1, int outputIndex, ExecutionTask task2, int inputIndex) {
        final Class<Channel> channelClass = (Class<Channel>) this.pickChannelClass(task1, outputIndex, task2, inputIndex);
        if (channelClass == null) {
            return false;
        }

        Channel channel = task1.getOutputChannel(outputIndex);
        if (channel == null) {
            channel = this.setUpOutput(task1, outputIndex, channelClass);
        }
        this.setUpInput(task2, inputIndex, channelClass, channel);
        return true;
    }

    private void setUpInput(ExecutionTask task2, int inputIndex, Class<Channel> channelClass, Channel channel) {
        final ChannelInitializer<Channel> channelInitializer = task2.getOperator().getPlatform().getChannelInitializer(channelClass);
        channelInitializer.setUpInput(channel, task2, inputIndex);
    }

    private Channel setUpOutput(ExecutionTask task1, int outputIndex, Class<? extends Channel> channelClass) {
        final ChannelInitializer<? extends Channel> channelInitializer = task1.getOperator().getPlatform().getChannelInitializer(channelClass);
        return channelInitializer.setUpOutput(task1, outputIndex);
    }

    private Class<? extends Channel> pickChannelClass(ExecutionTask task1, int outputIndex, ExecutionTask task2, int inputIndex) {
        final ExecutionOperator op1 = task1.getOperator();
        final ExecutionOperator op2 = task2.getOperator();
        final boolean requestReusableChannel = op1.getOutermostOutputSlots(op1.getOutput(outputIndex)).stream()
                .flatMap(outputSlot -> outputSlot.getOccupiedSlots().stream())
                .count() > 1;
        final List<Class<? extends Channel>> supportedOutputChannels = op1.getSupportedOutputChannels(outputIndex);
        final List<Class<? extends Channel>> supportedInputChannels = op2.getSupportedInputChannels(inputIndex);
        for (Class<? extends Channel> channelClass : supportedOutputChannels) {
            if (requestReusableChannel && !this.checkIfReusable(op1, channelClass)) continue;
            if (supportedInputChannels.contains(channelClass)) {
                return channelClass;
            }
        }
        return null;
    }

    private boolean checkIfReusable(ExecutionOperator operator, Class<? extends Channel> channelClass) {
        final ChannelInitializer<? extends Channel> channelInitializer = operator.getPlatform().getChannelInitializer(channelClass);
        return channelInitializer.isReusable();
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
        protected boolean doWork() {
            this.executionTask = new ExecutionTask((ExecutionOperator) this.operator);
            for (int inputIndex = 0; inputIndex < this.activations.length; inputIndex++) {
                final Activation activation = this.activations[inputIndex];
                final ExecutionTask predecessorTask = activation.executionTask;
                boolean isConnected = ExecutionPlanCreator.this.connect(
                        predecessorTask, activation.outputIndex, this.executionTask, activation.inputIndex);
                if (!isConnected) {
                    ExecutionPlanCreator.this.logger.info("Could not connect {} to {}.", predecessorTask, this.executionTask);
                    return false;
                }
            }
            return true;
        }

        @Override
        protected Collection<Activation> getSuccessorActivations() {
            // Make sure, we are set up correctly.
            Validate.notNull(this.executionTask);

            // For each outermost OutputSlot of this instance's #operator...
            Collection<Activation> collector = new LinkedList<>();
            for (int outputIndex = 0; outputIndex < this.operator.getNumOutputs(); outputIndex++) {
                final Collection<OutputSlot<Object>> outputs =
                        this.operator.getOutermostOutputSlots(this.operator.getOutput(outputIndex).unchecked());
                for (OutputSlot<Object> output : outputs) {
                    // ...and for each InputSlot fed by such an OutputSlot...
                    for (InputSlot<Object> input : output.getOccupiedSlots()) {
                        // ...add a new Activation.
                        this.createActivation(outputIndex, input, collector);
                    }
                }
            }

            // If we could not create any Activation, then we safe the current operator.
            if (collector.isEmpty()) {
                ExecutionPlanCreator.this.terminalTasks.add(this.executionTask);
            }

            return collector;
        }

        private void createActivation(int thisOutputIndex, InputSlot<Object> targetInput, Collection<Activation> collector) {
            final Operator targetOperator = targetInput.getOwner();
            if (targetOperator.isAlternative()) {
                OperatorAlternative.Alternative alternative =
                        ExecutionPlanCreator.this.partialPlan.getChosenAlternative((OperatorAlternative) targetOperator);
                if (alternative != null) {
                    final Collection<InputSlot<Object>> innerTargetInputs = alternative.followInput(targetInput);
                    for (InputSlot<Object> innerTargetInput : innerTargetInputs) {
                        this.createActivation(thisOutputIndex, innerTargetInput, collector);
                    }
                }
            } else if (targetOperator.isExecutionOperator()) {
                final Activator activator =
                        ExecutionPlanCreator.this.activators.computeIfAbsent(targetOperator, Activator::new);
                collector.add(new Activation(activator, this.executionTask, thisOutputIndex, targetInput.getIndex()));
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

        private final int inputIndex;

        private final int outputIndex;

        private final ExecutionTask executionTask;

        protected Activation(Activator targetActivator, ExecutionTask executionTask, int outputIndex, int inputIndex) {
            super(targetActivator);
            this.executionTask = executionTask;
            this.outputIndex = outputIndex;
            this.inputIndex = inputIndex;
        }

    }
}
