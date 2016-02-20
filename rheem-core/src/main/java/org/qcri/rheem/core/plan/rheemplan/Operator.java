package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.*;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An operator is any node that within a {@link RheemPlan}.
 * <p>An operator is basically determined by
 * <ul>
 * <li>its type,</li>
 * <li>its configuration,</li>
 * <li>its {@link InputSlot}s, and</li>
 * <li>its {@link OutputSlot}s.</li>
 * </ul>
 * The former two aspects are handled by subclassed, the latter two are basic features of every operator.</p>
 * <p>{@link Slot}s are typed input and output declarations of each operator and can be connected to each other
 * to form a full {@link RheemPlan}. Moreover, we distinguish between two kinds of {@link InputSlot}s:
 * <ol>
 * <li><b>Regular.</b>Each operator will have set up these {@link InputSlot}s already during its creation.
 * They are indexed from 0 to the number of {@link InputSlot}s - 1.</li>
 * <li><b>Broadcast.</b>Some operators permit for broadcast {@link InputSlot}s. These are dynamically added and
 * will be indexed after the regular ones. Also, their execution semantics differ: Broadcast input data will be
 * provided <i>before</i> the regular data.</li>
 * </ol></p>
 */
public interface Operator {

    /**
     * @return the number of {@link InputSlot}s of this instance; inclusive of broadcast {@link InputSlot}s
     */
    default int getNumInputs() {
        return this.getAllInputs().length;
    }

    /**
     * @return the number of non-broadcast {@link InputSlot}s of this instance
     */
    default int getNumRegularInputs() {
        return this.getNumInputs() - this.getNumBroadcastInputs();
    }

    /**
     * @return the number of broadcast {@link InputSlot}s of this instance
     */
    default int getNumBroadcastInputs() {
        return (int) Arrays.stream(this.getAllInputs()).filter(InputSlot::isBroadcast).count();
    }

    /**
     * @return the number of {@link OutputSlot}s of this instance
     */
    default int getNumOutputs() {
        return this.getAllOutputs().length;
    }

    /**
     * @return the {@link InputSlot}s of this instance; inclusive of broadcast {@link InputSlot}s
     */
    InputSlot<?>[] getAllInputs();

    /**
     * @return the {@link OutputSlot}s of this instance
     */
    OutputSlot<?>[] getAllOutputs();

    /**
     * Retrieve an {@link InputSlot} of this instance using its index.
     *
     * @param index of the {@link InputSlot}
     * @return the requested {@link InputSlot}
     */
    default InputSlot<?> getInput(int index) {
        final InputSlot[] allInputs = this.getAllInputs();
        Validate.inclusiveBetween(0, allInputs.length - 1, index, "Illegal input index %d for %s.", index, this);
        return allInputs[index];
    }

    /**
     * Retrieve an {@link OutputSlot} of this instance using its index.
     *
     * @param index of the {@link OutputSlot}
     * @return the requested {@link OutputSlot}
     */
    default OutputSlot<?> getOutput(int index) {
        final OutputSlot[] allOutputs = this.getAllOutputs();
        if (index < 0 || index >= allOutputs.length) {
            throw new IllegalArgumentException(String.format("Illegal output index: %d.", index));
        }
        return allOutputs[index];
    }

    /**
     * Retrieve an {@link InputSlot} of this instance by its name.
     *
     * @param name of the {@link InputSlot}
     * @return the requested {@link InputSlot}
     */
    default InputSlot<?> getInput(String name) {
        for (InputSlot inputSlot : this.getAllInputs()) {
            if (inputSlot.getName().equals(name)) return inputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    /**
     * Retrieve an {@link OutputSlot} of this instance by its name.
     *
     * @param name of the {@link OutputSlot}
     * @return the requested {@link OutputSlot}
     */
    default OutputSlot<?> getOutput(String name) {
        for (OutputSlot outputSlot : this.getAllOutputs()) {
            if (outputSlot.getName().equals(name)) return outputSlot;
        }
        throw new IllegalArgumentException(String.format("No slot with such name: %s", name));
    }

    /**
     * @return whether this instance permits broadcast {@link InputSlot}s besides their regular {@link InputSlot}s
     */
    boolean isSupportingBroadcastInputs();

    /**
     * Register an {@link InputSlot} as broadcast input of this instance.
     *
     * @param broadcastInput the {@link InputSlot} to be registered
     * @return the assigned index of the {@link InputSlot}
     */
    int addBroadcastInput(InputSlot<?> broadcastInput);

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
     * Connect an output of this operator as a broadcast input of a second operator.
     *
     * @param thisOutputIndex index of the output slot to connect to
     * @param that            operator to connect to
     * @param broadcastName   name of the broadcast that will be used by the operator to identify the broadcast; must
     *                        be unique among all {@link InputSlot}s
     */
    default void broadcastTo(int thisOutputIndex, Operator that, String broadcastName) {
        final OutputSlot<?> output = this.getOutput(thisOutputIndex);
        final InputSlot<?> broadcastInput = new InputSlot<>(broadcastName, that, true, output.getType());
        final int broadcastIndex = that.addBroadcastInput(broadcastInput);
        this.connectTo(thisOutputIndex, that, broadcastIndex);
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
        return this.getInputOperator(this.getInput(inputIndex));
    }

    /**
     * Retrieve the operator that is connected to the given input. If necessary, escape the current parent.
     *
     * @param input the index of the input to consider
     * @return the input operator or {@code null} if no such operator exists
     * @see #getParent()
     */
    default Operator getInputOperator(InputSlot<?> input) {
        if (!this.isOwnerOf(input)) {
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
        if (!this.isOwnerOf(input)) {
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
     * Retrieve the {@link InputSlot} of the enclosing {@link OperatorContainer} that represents the given one.
     *
     * @param input the slot to track
     * @return the outer {@link InputSlot} or {@code null} if none
     * @see #getParent()
     */
    default <T> InputSlot<T> getOuterInputSlot(InputSlot<T> input) {
        assert this.isOwnerOf(input);

        // Try to exit through the parent.
        final OperatorContainer container = this.getContainer();
        return container != null ? container.traceInput(input) : null;
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
        final OperatorContainer container = this.getContainer();
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
            final Configuration configuration) {
        return new DefaultCardinalityPusher(this, configuration.getCardinalityEstimatorProvider());
    }

    /**
     * Tells if this instance should be executed/implemented only on a certain set of {@link Platform}s.
     *
     * @return the targeted {@link Platform}s or an empty {@link Set} if there is no such restriction
     */
    Set<Platform> getTargetPlatforms();

    /**
     * <i>Optional operation.</i> Restrict this instance to be executed/implemented on a certain {@link Platform}s or
     * allow a further one if there is already a restriction in place.
     *
     * @return the targeted {@link Platform}s or an empty {@link Set} if there is no such restriction
     */
    void addTargetPlatform(Platform platform);

    /**
     * Set the {@link CardinalityEstimate} of an {@link OutputSlot} and propagate it to
     * <ul>
     * <li>fed {@link InputSlot}s (which in turn are asked to propagate) and</li>
     * <li>inner, mapped {@link OutputSlot}s.</li>
     * </ul>
     */
    void propagateOutputCardinality(int outputIndex, CardinalityEstimate cardinalityEstimate);

    /**
     * Set the {@link CardinalityEstimate} of an {@link InputSlot} and propagate it to inner, mapped {@link InputSlot}s.
     */
    void propagateInputCardinality(int inputIndex, CardinalityEstimate cardinalityEstimate);

}
