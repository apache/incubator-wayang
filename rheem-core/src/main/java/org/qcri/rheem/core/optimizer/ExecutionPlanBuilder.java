package org.qcri.rheem.core.optimizer;

import org.qcri.rheem.core.plan.*;
import org.qcri.rheem.core.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class can be used to build an execution {@link org.qcri.rheem.core.plan.PhysicalPlan} in a bottom-up manner.
 * @deprecated
 */
public class ExecutionPlanBuilder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * This map keeps track of which (potentially alternative) outputs are already implemented via an {@link
     * ExecutionOperator}. Keys are non-wrapped {@link OutputSlot}s of the processed
     * hyperplan, while values are {@link OutputSlot}s of the picked {@link ExecutionOperator}s.
     */
    private final Map<OutputSlot, OutputSlot> servedOutputSlots;

    private final Collection<ExecutionOperator> sinks;

    private final Collection<ExecutionOperator> allOperators;

    public ExecutionPlanBuilder() {
        this(new IdentityHashMap<>(), new LinkedList<>(), new LinkedList<>());
    }

    private ExecutionPlanBuilder(
            Map<OutputSlot, OutputSlot> servedOutputSlots,
            Collection<ExecutionOperator> sinks,
            Collection<ExecutionOperator> allOperators) {
        this.servedOutputSlots = servedOutputSlots;
        this.sinks = sinks;
        this.allOperators = allOperators;
    }

    /**
     * Try to find the {@link OutputSlot} that serves the given {@link InputSlot}.
     *
     * @param inputSlot the {@link InputSlot} that should be served
     * @return the previously built {@link OutputSlot} that serves the given {@code inputSlot} or {@code null} if no
     * such {@link OutputSlot} exists
     */
    public Optional<OutputSlot> getServingOutputSlot(InputSlot inputSlot) {
        // Resolve the outermost input slot.
        final InputSlot outermostInput = InputSlot.traceOutermostInput(inputSlot);
        if (outermostInput == null) {
            return Optional.of(null);
        }

        final OutputSlot occupant = outermostInput.getOccupant();
        if (occupant == null) {
            return Optional.of(null);
        }

        final OutputSlot<?> servingOutputSlot = this.servedOutputSlots.get(occupant);
        return servingOutputSlot == null ?
                Optional.na() :
                Optional.of(servingOutputSlot);

    }

    public void add(ExecutionOperator executionOperator, OutputSlot[] servingOutputSlots) {
        // Create a personal copy of the ExecutionOperator and connect it to the OutputSlots.
        final ExecutionOperator buildersOperator = executionOperator.copy();
        for (int i = 0; i < executionOperator.getNumInputs(); i++) {
            servingOutputSlots[i].connectTo(buildersOperator.getInput(i));
        }

        // Register the OutputSlots of the copied ExecutionOperator.
        for (int i = 0; i < executionOperator.getNumOutputs(); i++) {
            final OutputSlot servingOutput = buildersOperator.getOutput(i);
            OutputSlot.followOutputRecursively(executionOperator.getOutput(i)).stream()
                    .forEach(nonWrappedOutput -> this.servedOutputSlots.put(nonWrappedOutput, servingOutput));
        }

        this.allOperators.add(buildersOperator);

        if (buildersOperator.isSink()) {
            this.sinks.add(buildersOperator);
        }
    }

    public Optional<PhysicalPlan> build() {
        // See if there are sinks in the first place.
        if (this.sinks.isEmpty()) {
            logger.info("Discard plan without sinks.");
            return Optional.na();
        }

        // Collect all parts of the plan that are reachable from the sinks, thereby ensuring that all these operators
        // furthermore have their inputs satisfied.
        final AtomicBoolean isAllConsumedOperatorInputsSatisfied = new AtomicBoolean(true);
        final Collection<Operator> consumedOperators = new HashSet<>(
                new PlanTraversal(true, false)
                        .withCallback(
                                (operator, fromInputSlot, fromOutputSlot) -> {
                                    if (Arrays.stream(operator.getAllInputs()).anyMatch(inputSlot -> Objects.isNull(inputSlot.getOccupant()))) {
                                        isAllConsumedOperatorInputsSatisfied.set(false);
                                    }
                                })
                        .traverse(this.sinks)
                        .getTraversedNodesWith(operator -> true)
        );
        if (!isAllConsumedOperatorInputsSatisfied.get()) {
            logger.info("Discard plan that comprises consumed operators with unsatisfied inputs.");
            return Optional.na();
        }

        // Prune operators that are not leading to a sink.
        new PlanTraversal(true, false)
                .withCallback((operator, fromInputSlot, fromOutputSlot) -> {
                            for (OutputSlot<?> outputSlot : operator.getAllOutputs()) {
                                final ArrayList<InputSlot<?>> inputSlots = new ArrayList<>(outputSlot.getOccupiedSlots());
                                for (InputSlot<?> inputSlot : inputSlots) {
                                    if (!consumedOperators.contains(inputSlot.getOwner())) {
                                        outputSlot.unchecked().disconnectFrom(inputSlot.unchecked());
                                    }
                                }
                            }
                        }
                ).traverse(this.sinks);

        // Build the plan.
        final PhysicalPlan plan = new PhysicalPlan();
        this.sinks.forEach(plan::addSink);
        return Optional.of(plan);
    }

    /**
     * @return a deep copy of this instance
     */
    public ExecutionPlanBuilder copy() {

        final Collection<ExecutionOperator> allOperatorsCopy = new LinkedList<>();
        final Collection<ExecutionOperator> sinksCopy = new LinkedList<>();

        // Maintain a dictionary from original to copied operators.
        Map<Slot, Slot> slotDictionary = new HashMap<>();

        // Copy all operators, thereby maintaining connections.
        for (ExecutionOperator originalOperator : this.allOperators) {
            final ExecutionOperator copiedOperator = originalOperator.copy();
            for (int i = 0; i < originalOperator.getNumInputs(); i++) {
                final InputSlot<?> originalInput = originalOperator.getInput(i);
                final InputSlot<?> copiedInput = copiedOperator.getInput(i);
                slotDictionary.put(originalInput, copiedInput);

                if (originalInput.getOccupant() == null) continue;
                final OutputSlot translatedOccupant = (OutputSlot) slotDictionary.get(originalInput.getOccupant());
                if (translatedOccupant != null) {
                    translatedOccupant.connectTo(copiedInput);
                }
            }

            for (int i = 0; i < originalOperator.getNumOutputs(); i++) {
                final OutputSlot originalOutput = originalOperator.getOutput(i);
                final OutputSlot copiedOutput = copiedOperator.getOutput(i);
                slotDictionary.put(originalOutput, copiedOutput);

                for (InputSlot originalOccupiedSlot : (List<InputSlot>) originalOutput.getOccupiedSlots()) {
                    final InputSlot translatedOccupiedSlot = (InputSlot) slotDictionary.get(originalOccupiedSlot);
                    if (translatedOccupiedSlot != null) {
                        copiedOutput.connectTo(translatedOccupiedSlot);
                    }
                }
            }

            allOperatorsCopy.add(copiedOperator);
            if (copiedOperator.isSink()) {
                sinksCopy.add(copiedOperator);
            }
        }

        // Translate the exposed sinks.
        Map<OutputSlot, OutputSlot> servedOutputSlotsCopy = new HashMap<>();
        for (Map.Entry<OutputSlot, OutputSlot> entry : this.servedOutputSlots.entrySet()) {
            servedOutputSlotsCopy.put(entry.getKey(), (OutputSlot) slotDictionary.get(entry.getValue()));
        }

        return new ExecutionPlanBuilder(servedOutputSlotsCopy, sinksCopy, allOperatorsCopy);
    }
}
