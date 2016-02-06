package org.qcri.rheem.core.plan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.*;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An operator is any node that within a data flow plan.
 */
public interface Operator {

    default int getNumInputs() {
        return getAllInputs().length;
    }

    default int getNumOutputs() {
        return getAllOutputs().length;
    }

    InputSlot<?>[] getAllInputs();

    OutputSlot<?>[] getAllOutputs();

    default InputSlot<?> getInput(int index) {
        final InputSlot[] allInputs = getAllInputs();
        if (index < 0 || index >= allInputs.length) {
            throw new IllegalArgumentException(String.format("Illegal input index: %d.", index));
        }
        return allInputs[index];
    }

    default OutputSlot<?> getOutput(int index) {
        final OutputSlot[] allOutputs = getAllOutputs();
        if (index < 0 || index >= allOutputs.length) {
            throw new IllegalArgumentException(String.format("Illegal output index: %d.", index));
        }
        return allOutputs[index];
    }

    default InputSlot<?> getInput(String name) {
        for (InputSlot inputSlot : getAllInputs()) {
            if (inputSlot.getName().equals(name)) return inputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    default OutputSlot<?> getOutput(String name) {
        for (OutputSlot outputSlot : getAllOutputs()) {
            if (outputSlot.getName().equals(name)) return outputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    /**
     * Connect an output of this operator to the input of a second operator.
     *
     * @param thisOutputIndex index of the output slot to connect to
     * @param that            operator to connect to
     * @param thatInputIndex  index of the input slot to connect from
     */
    default <T> void connectTo(int thisOutputIndex, Operator that, int thatInputIndex) {
        final InputSlot<T> inputSlot = (InputSlot<T>) that.getInput(thatInputIndex);
        final OutputSlot<T> outputSlot = (OutputSlot<T>) this.getOutput(thisOutputIndex);
        if (!inputSlot.getType().isCompatibleTo(outputSlot.getType())) {
            throw new IllegalArgumentException("Cannot connect slots: mismatching types");
        }
        outputSlot.connectTo(inputSlot);
    }


    /**
     * Retrieve the operator that is connected to the input at the given index. If necessary, escape the current
     * parent.
     *
     * @param inputIndex the index of the input to consider
     * @return the input operator or {@code null} if no such operator exists
     * @see #getParent()
     */
    default Operator getInputOperator(int inputIndex) {
        return getInputOperator(getInput(inputIndex));
    }

    /**
     * Retrieve the operator that is connected to the given input. If necessary, escape the current parent.
     *
     * @param input the index of the input to consider
     * @return the input operator or {@code null} if no such operator exists
     * @see #getParent()
     */
    default Operator getInputOperator(InputSlot<?> input) {
        if (!isOwnerOf(input)) {
            throw new IllegalArgumentException("Slot does not belong to this operator.");
        }

        // Try to exit through the parent.
        final Operator parent = this.getParent();
        if (parent != null) {
            if (parent instanceof Subplan) {
                final InputSlot<?> exitInputSlot = ((Subplan) parent).exit(input);
                if (exitInputSlot != null) {
                    return parent.getInputOperator(exitInputSlot);
                }

            } else if (parent instanceof OperatorAlternative.Alternative) {
                final InputSlot<?> exitInputSlot = ((OperatorAlternative.Alternative) parent).exit(input);
                if (exitInputSlot != null) {
                    return parent.getInputOperator(exitInputSlot);
                }
            }

        }

        final OutputSlot occupant = input.getOccupant();
        return occupant == null ? null : occupant.getOwner();
    }

    /**
     * Retrieve the outermost {@link InputSlot} if this operator is nested in other operators.
     *
     * @param input the slot to track
     * @return the outermost {@link InputSlot}
     * @see #getParent()
     */
    default <T> InputSlot<T> getOutermostInputSlot(InputSlot<T> input) {
        if (!isOwnerOf(input)) {
            throw new IllegalArgumentException("Slot does not belong to this operator.");
        }

        if (input.getOccupant() != null) {
            return input;
        }

        // Try to exit through the parent.
        final OperatorContainer container = this.getContainer();
        if (container != null) {
            final InputSlot<T> tracedInput = container.traceInput(input);
            if (tracedInput != null) {
                return container.toOperator().getOutermostInputSlot(tracedInput);
            }
        }

        return input;
    }

    /**
     * Retrieve the outermost {@link OutputSlot}s if this operator is nested in other operators.
     *
     * @param output the slot to track
     * @return the outermost {@link InputSlot}
     * @see #getParent()
     */
    default <T> Collection<OutputSlot<T>> getOutermostOutputSlots(OutputSlot<T> output) {
        Validate.isTrue(this.isOwnerOf(output));

        if (!output.getOccupiedSlots().isEmpty()) {
            return Collections.singleton(output);
        }

        // Try to exit through the parent.
        final OperatorContainer container = this.getContainer();
        if (container != null) {
            final Collection<OutputSlot<T>> followedOutputs = container.followOutput(output);
            if (!followedOutputs.isEmpty()) {
                return followedOutputs.stream()
                        .flatMap(followedOutput -> container.toOperator().getOutermostOutputSlots(followedOutput).stream())
                        .collect(Collectors.toList());
            }
        }

        return Collections.singleton(output);
    }


    //    /**
//     * This method is part of the visitor pattern and calls the appropriate visit method on {@code visitor}.
//     *
//     * @return return values associated with the inner-most input slot
//     */

    default boolean isOwnerOf(Slot<?> slot) {
        return slot.getOwner() == this;
    }

    /**
     * Tells whether this operator is a sink, i.e., it has no outputs.
     *
     * @return whether this operator is a sink
     */
    default boolean isSink() {
        return this.getNumOutputs() == 0;
    }

    /**
     * Tells whether this operator is a source, i.e., it has no inputs.
     *
     * @return whether this operator is a source
     */
    default boolean isSource() {
        return this.getNumInputs() == 0;
    }

    default boolean isSubplan() {
        return this instanceof Subplan;
    }

    default boolean isAlternative() {
        return this instanceof OperatorAlternative;
    }

    default boolean isExecutionOperator() {
        return this instanceof ExecutionOperator;
    }

    /**
     * This method is part of the visitor pattern and calls the appropriate visit method on {@code visitor}.
     */
    <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload);

//    <Payload, Return> Map<InputSlot, Return> accept(BottomUpPlanVisitor<Payload, Return> visitor, InputSlot<?> inputSlot, Payload payload);

    /**
     * Operators can be nested in other operators, e.g., in a {@link Subplan} or a {@link OperatorAlternative}.
     *
     * @return the parent of this operator or {@code null} if this is a top-level operator
     */
    default CompositeOperator getParent() {
        final OperatorContainer container = getContainer();
        return container == null ?
                null :
                container.toOperator();
    }

    OperatorContainer getContainer();

    /**
     * Operators can be nested in other operators, e.g., in a {@link Subplan} or a {@link OperatorAlternative.Alternative}.
     *
     * @param newContainer the new container of this operator or {@code null} to declare it top-level
     */
    void setContainer(OperatorContainer newContainer);

    /**
     * Each operator is associated with an epoch, which is a logical timestamp for the operator's creation.
     * This value is the lowest timestamp and default epoch.
     */
    int FIRST_EPOCH = 0;

    /**
     * <i>Optional operation for non-composite operators.</i>
     *
     * @param epoch the operator's new epoch value
     * @see #FIRST_EPOCH
     */
    void setEpoch(int epoch);


    /**
     * <i>Optional operation for non-composite operators.</i>
     *
     * @return the operator's epoch value
     * @see #FIRST_EPOCH
     */
    int getEpoch();

    /**
     * @return whether this is an elementary operator
     */
    default boolean isElementary() {
        return true;
    }


    /**
     * Provide a {@link CardinalityEstimator} for the {@link OutputSlot} at {@code outputIndex}.
     *
     * @param outputIndex   index of the {@link OutputSlot} for that the {@link CardinalityEstimator} is requested
     * @param configuration if the {@link CardinalityEstimator} depends on further ones, use this to obtain the latter
     * @return an {@link Optional} that might provide the requested instance
     */
    default Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        LoggerFactory.getLogger(this.getClass()).warn("Use fallback cardinality estimator for {}.", this);
        return Optional.of(new FallbackCardinalityEstimator());
    }

    /**
     * Provide a {@link CardinalityPusher} for the {@link Operator}.
     *
     * @param configuration if the {@link CardinalityPusher} depends on further ones, use this to obtain the latter
     * @return the {@link CardinalityPusher}
     */
    default CardinalityPusher getCardinalityPusher(
            final Configuration configuration,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        return new DefaultCardinalityPusher(this, configuration.getCardinalityEstimatorProvider(), cache);
    }
}
