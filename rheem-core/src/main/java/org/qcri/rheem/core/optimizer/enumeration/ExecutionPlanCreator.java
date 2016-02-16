package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
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

    private final Map<ExecutionOperator, Activator> activators = new HashMap<>();

    private final Collection<Activator> startActivators;

    private final PartialPlan partialPlan;

    private final Collection<ExecutionTask> terminalTasks = new LinkedList<>();

    private final Map<ExecutionOperator, ExecutionTask> executionTasks = new HashMap<>();

    public ExecutionPlanCreator(Collection<ExecutionOperator> startOperators, PartialPlan partialPlan) {
        this.partialPlan = partialPlan;
        this.startActivators = startOperators.stream().map(Activator::new).collect(Collectors.toList());
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
                if (!this.establishCollections(outputIndex, platform, collector)) return null;
            }

            // If we could not create any Activation, then we safe the current operator.
            if (collector.isEmpty()) {
                ExecutionPlanCreator.this.terminalTasks.add(this.executionTask);
            }

            return collector;
        }

        private boolean establishCollections(int outputIndex, Platform platform, Collection<Activation> collector) {
            // Collect all InputSlots that are connected to the current OutputSlot.
            final List<InputSlot<Object>> targetInputs = this.operator
                    .getOutermostOutputSlots(this.operator.getOutput(outputIndex).unchecked())
                    .stream()
                    .flatMap(output -> output.getOccupiedSlots().stream())
                    .flatMap(this::findExecutionOperatorInputs)
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

        private Stream<? extends InputSlot<Object>> findExecutionOperatorInputs(InputSlot<Object> input) {
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
            return alternative.followInput(input).stream().flatMap(this::findExecutionOperatorInputs);
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
            Validate.isTrue(this.activations[activation.inputIndex] == null);
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
