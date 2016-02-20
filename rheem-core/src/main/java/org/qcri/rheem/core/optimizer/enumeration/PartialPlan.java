package org.qcri.rheem.core.optimizer.enumeration;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.plan.rheemplan.traversal.AbstractTopologicalTraversal;
import org.qcri.rheem.core.util.Canonicalizer;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a partial execution plan.
 */
public class PartialPlan {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialPlan.class);

    /**
     * {@link ExecutionOperator}s contained in this instance.
     */
    private final Canonicalizer<ExecutionOperator> operators;

    /**
     * An enumerated plan is mainly characterized by the {@link OperatorAlternative.Alternative}s that have
     * been picked so far. This member keeps track of them.
     */
    private final Map<OperatorAlternative, OperatorAlternative.Alternative> settledAlternatives =
            new HashMap<>();

    /**
     * The {@link PlanEnumeration} that hosts this instance. Can change over time.
     */
    private PlanEnumeration planEnumeration;

    /**
     * <i>Lazy-initialized.</i> {@link PreliminaryExecutionPlan} representation of this instance.
     */
    private PreliminaryExecutionPlan executionPlan;

    /**
     * Create a new instance.
     */
    PartialPlan(
            PlanEnumeration planEnumeration,
            Collection<ExecutionOperator> operators) {
        this.planEnumeration = planEnumeration;
        this.operators = new Canonicalizer<>(operators);
    }


    PartialPlan(PlanEnumeration planEnumeration, Collection<ExecutionOperator>... operatorCollections) {
        this.planEnumeration = planEnumeration;
        this.operators = new Canonicalizer<>();
        for (Collection<ExecutionOperator> operatorCollection : operatorCollections) {
            this.operators.addAll(operatorCollection);
        }
    }

    /**
     * @return the {@link PlanEnumeration} this instance belongs to
     */
    public PlanEnumeration getPlanEnumeration() {
        return this.planEnumeration;
    }

    public void setPlanEnumeration(PlanEnumeration planEnumeration) {
        this.planEnumeration = planEnumeration;
    }

    /**
     * Create a new instance that forms the concatenation of the two.
     *
     * @param that        instance to join with
     * @param commonScope {@link OperatorAlternative}s that are selected in both instances
     * @param target
     * @return the joined instance or {@code null} if the two input instances disagree in some {@link OperatorAlternative}s
     */
    public PartialPlan join(PartialPlan that, List<OperatorAlternative> commonScope, PlanEnumeration target) {

        // Find out if the two plans do not disagree at some point.
        for (OperatorAlternative operatorAlternative : commonScope) {
            final OperatorAlternative.Alternative thisChosenAlternative = this.settledAlternatives.get(operatorAlternative);
            final OperatorAlternative.Alternative thatChosenAlternative = that.settledAlternatives.get(operatorAlternative);
            if (!thatChosenAlternative.equals(thisChosenAlternative)) {
                LOGGER.trace("Cannot combine two partial plans: they disagree in some alternatives");
                return null;
            }
        }

        final PartialPlan partialPlan = new PartialPlan(target);
        partialPlan.operators.addAll(this.operators);
        partialPlan.operators.addAll(that.operators);
        partialPlan.settledAlternatives.putAll(this.settledAlternatives);
        partialPlan.settledAlternatives.putAll(that.settledAlternatives);

        return partialPlan;
    }

    /**
     * Escapes the {@link OperatorAlternative} that contains this instance.
     *
     * @param alternative        contains this instance
     * @param newPlanEnumeration will host the new instance
     * @return
     */
    public PartialPlan escape(OperatorAlternative.Alternative alternative, PlanEnumeration newPlanEnumeration) {
        final PartialPlan escapedPartialPlan = new PartialPlan(newPlanEnumeration, this.operators);
        escapedPartialPlan.settledAlternatives.putAll(this.settledAlternatives);
        escapedPartialPlan.settledAlternatives.put(alternative.getOperatorAlternative(), alternative);
        return escapedPartialPlan;
    }

    public Canonicalizer<ExecutionOperator> getOperators() {
        return this.operators;
    }

    /**
     * @return those contained {@link ExecutionOperator}s that have a {@link Slot} that is yet to be connected
     * to a further {@link ExecutionOperator} in the further plan enumeration process
     */
    public Collection<ExecutionOperator> getInterfaceOperators() {
        Validate.notNull(this.getPlanEnumeration());
        final Set<OutputSlot> outputSlots = this.getPlanEnumeration().servingOutputSlots.stream()
                .map(Tuple::getField0)
                .distinct()
                .collect(Collectors.toSet());
        final Set<InputSlot> inputSlots = this.getPlanEnumeration().requestedInputSlots;

        return this.operators.stream()
                .filter(operator ->
                        this.allOutermostInputSlots(operator).anyMatch(inputSlots::contains) ||
                                this.allOutermostOutputSlots(operator).anyMatch(outputSlots::contains))
                .collect(Collectors.toList());
    }

    private Stream<OutputSlot> allOutermostOutputSlots(Operator operator) {
        return Arrays.stream(operator.getAllOutputs())
                .flatMap(output -> operator.getOutermostOutputSlots(output).stream());
    }

    private Stream<InputSlot> allOutermostInputSlots(Operator operator) {
        return Arrays.stream(operator.getAllInputs())
                .map(operator::getOutermostInputSlot);
    }

    public PreliminaryExecutionPlan getExecutionPlan() {
        assert this.executionPlan != null;
        return this.executionPlan;
    }

    public PreliminaryExecutionPlan createExecutionPlan() {
        if (this.executionPlan == null) {
            final List<ExecutionOperator> startOperators = this.operators.stream()
                    .filter(this::isStartOperator)
                    .collect(Collectors.toList());
            assert !startOperators.isEmpty() :
                    String.format("Could not find start operators among %s: none provides any of %s.",
                            this.operators, this.planEnumeration.requestedInputSlots);
            final ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(startOperators, this);
            if (executionPlanCreator.traverse()) {
                this.executionPlan = new PreliminaryExecutionPlan(executionPlanCreator.getTerminalTasks());
            }
        }
        return this.executionPlan;
    }

    public PreliminaryExecutionPlan createExecutionPlan(
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages) {
        if (this.executionPlan == null) {
            final List<ExecutionOperator> startOperators = this.operators.stream()
                    .filter(this::isStartOperator)
                    .collect(Collectors.toList());
            assert !startOperators.isEmpty() :
                    String.format("Could not find start operators among %s: none provides any of %s.",
                            this.operators, this.planEnumeration.requestedInputSlots);
            try {
                final ExecutionPlanCreator executionPlanCreator = new ExecutionPlanCreator(
                        startOperators, this, existingPlan, openChannels, executedStages);
                if (executionPlanCreator.traverse((Void[]) null)) {
                    this.executionPlan = new PreliminaryExecutionPlan(executionPlanCreator.getTerminalTasks(),
                            executionPlanCreator.getInputChannels());
                }
            } catch (AbstractTopologicalTraversal.AbortException e) {
            }
        }
        return this.executionPlan;
    }

    /**
     * Detects start {@link ExecutionOperator}s.
     * <p>A start {@link ExecutionOperator} has an {@link InputSlot} that is requested by the {@link #planEnumeration}.</p>
     */
    private boolean isStartOperator(ExecutionOperator executionOperator) {
        ForLoop:
        for (InputSlot<?> inputSlot : executionOperator.getOriginal().getAllInputs()) {
            while (inputSlot != null) {
                if (this.planEnumeration.requestedInputSlots.contains(inputSlot)) {
                    continue ForLoop;
                }
                inputSlot = inputSlot.getOwner().getOuterInputSlot(inputSlot);
            }
            return false;
        }
        return true;
    }

//    public RheemPlan toRheemPlan() {
//        Map<OutputSlot<?>, ExecutionOperator> copiedOutputProviders = new HashMap<>();
//        Map<OutputSlot<?>, Collection<InputSlot<?>>> copiedOutputRequesters = new HashMap<>();
//
//        Collection<ExecutionOperator> sinks = new LinkedList<>();
//        for (ExecutionOperator pickedOperator : this.operators) {
//            final ExecutionOperator copy = pickedOperator.copy();
//
//            // Connect the InputSlots or issue an connecting request.
//            for (InputSlot<?> originalInput : pickedOperator.getAllInputs()) {
//                final InputSlot<?> originalOuterInput = pickedOperator.getOutermostInputSlot(originalInput);
//                final OutputSlot<?> originalRequestedOutput = originalOuterInput.getOccupant();
//                Validate.notNull(originalRequestedOutput, "Outermost %s does not have an occupant.", originalOuterInput);
//                final ExecutionOperator copiedOutputProvider = copiedOutputProviders.get(originalRequestedOutput);
//                if (copiedOutputProvider != null) {
//                    copiedOutputProvider.connectTo(originalRequestedOutput.getIndex(),
//                            copy,
//                            originalInput.getIndex());
//                } else {
//                    RheemCollections.put(copiedOutputRequesters,
//                            originalRequestedOutput,
//                            copy.getInput(originalInput.getIndex()));
//                }
//            }
//
//            // Connect the OutputSlots and offer further connections.
//            for (OutputSlot<?> originalOutput : pickedOperator.getAllOutputs()) {
//                Collection<OutputSlot<Object>> originalOuterOutputs =
//                        pickedOperator.getOutermostOutputSlots(originalOutput.unchecked());
//                for (OutputSlot<Object> originalOuterOutput : originalOuterOutputs) {
//                    copiedOutputProviders.put(originalOuterOutput, copy);
//                    final Collection<InputSlot<?>> connectableOutputRequesters =
//                            copiedOutputRequesters.remove(originalOuterOutput);
//                    if (connectableOutputRequesters != null) {
//                        for (InputSlot<?> outputRequester : connectableOutputRequesters) {
//                            copy.connectTo(originalOutput.getIndex(),
//                                    outputRequester.getOwner(),
//                                    outputRequester.getIndex());
//                        }
//                    }
//                }
//            }
//
//            // Remember sinks.
//            if (copy.isSink()) {
//                sinks.add(copy);
//            }
//        }
//        Validate.isTrue(copiedOutputRequesters.isEmpty());
//        Validate.isTrue(!sinks.isEmpty());
//
//        final RheemPlan rheemPlan = new RheemPlan();
//        sinks.forEach(rheemPlan::addSink);
//        return rheemPlan;
//    }

    /**
     * Find for a given {@link OperatorAlternative}, which {@link OperatorAlternative.Alternative} has been picked
     * by this instance
     *
     * @param operatorAlternative the {@link OperatorAlternative} in question
     * @return the {@link OperatorAlternative.Alternative} or {@code null} if none has been chosen in this instance
     */
    public OperatorAlternative.Alternative getChosenAlternative(OperatorAlternative operatorAlternative) {
        return this.settledAlternatives.get(operatorAlternative);
    }
}
