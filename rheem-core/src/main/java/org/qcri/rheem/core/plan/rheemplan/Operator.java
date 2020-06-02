package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityPusher;
import org.qcri.rheem.core.platform.Platform;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
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
 * The former two aspects are handled by subclassed, the latter two are basic features of every operator.
 * <p>{@link Slot}s are typed input and output declarations of each operator and can be connected to each other
 * to form a full {@link RheemPlan}. Moreover, we distinguish between two kinds of {@link InputSlot}s:
 * <ol>
 * <li><b>Regular.</b>Each operator will have set up these {@link InputSlot}s already during its creation.
 * They are indexed from 0 to the number of {@link InputSlot}s - 1.</li>
 * <li><b>Broadcast.</b>Some operators permit for broadcast {@link InputSlot}s. These are dynamically added and
 * will be indexed after the regular ones. Also, their execution semantics differ: Broadcast input data will be
 * provided <i>before</i> the regular data.</li>
 * </ol>
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
        return (int) Arrays.stream(this.getAllInputs())
                .filter(Objects::nonNull)
                .filter(InputSlot::isBroadcast)
                .count();
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
     * Sets the {@link InputSlot} of this instance. This method must only be invoked, when the input index is not
     * yet filled.
     *
     * @param index at which the {@link InputSlot} should be placed
     * @param input the new {@link InputSlot}
     */
    default void setInput(int index, InputSlot<?> input) {
        assert index < this.getNumRegularInputs() && this.getInput(index) == null;
        assert input.getOwner() == this;
        ((InputSlot[]) this.getAllInputs())[index] = input;
    }

    /**
     * Sets the {@link OutputSlot} of this instance. This method must only be invoked, when the output index is not
     * yet filled.
     *
     * @param index  at which the {@link OutputSlot} should be placed
     * @param output the new {@link OutputSlot}
     */
    default void setOutput(int index, OutputSlot<?> output) {
        assert index < this.getNumOutputs() && this.getOutput(index) == null;
        assert output.getOwner() == this;
        ((OutputSlot[]) this.getAllOutputs())[index] = output;
    }

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
    @SuppressWarnings("unchecked")
    default <T> void connectTo(int thisOutputIndex, Operator that, int thatInputIndex) {
        final InputSlot<T> inputSlot = (InputSlot<T>) that.getInput(thatInputIndex);
        final OutputSlot<T> outputSlot = (OutputSlot<T>) this.getOutput(thisOutputIndex);
        if (!inputSlot.getType().isSupertypeOf(outputSlot.getType())) {
            throw new IllegalArgumentException(String.format(
                    "Cannot connect %s of %s to %s of type %s.",
                    outputSlot, outputSlot.getType(), inputSlot, inputSlot.getType()));
        }
        outputSlot.connectTo(inputSlot);
    }

    /**
     * Connect an output of this operator to the input of a second operator.
     *
     * @param thisOutputName name of the output slot to connect to
     * @param that           operator to connect to
     * @param thatInputName  name of the input slot to connect from
     */
    @SuppressWarnings("unchecked")
    default <T> void connectTo(String thisOutputName, Operator that, String thatInputName) {
        final InputSlot<T> inputSlot = (InputSlot<T>) that.getInput(thatInputName);
        final OutputSlot<T> outputSlot = (OutputSlot<T>) this.getOutput(thisOutputName);
        if (!inputSlot.getType().isSupertypeOf(outputSlot.getType())) {
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
     * Connect an output of this operator as a broadcast input of a second operator.
     *
     * @param thisOutputName name of the output slot to connect to
     * @param that           operator to connect to
     * @param broadcastName  name of the broadcast that will be used by the operator to identify the broadcast; must
     *                       be unique among all {@link InputSlot}s
     */
    default void broadcastTo(String thisOutputName, Operator that, String broadcastName) {
        final OutputSlot<?> output = this.getOutput(thisOutputName);
        final InputSlot<?> broadcastInput = new InputSlot<>(broadcastName, that, true, output.getType());
        final int broadcastIndex = that.addBroadcastInput(broadcastInput);
        this.connectTo(output.getIndex(), that, broadcastIndex);
    }

    /**
     * Retrieves the effective occupant of the given {@link InputSlot}, i.e., the {@link OutputSlot} that is
     * either connected to the given or an outer-more, mapped {@link InputSlot}.
     *
     * @param inputIndex of the {@link InputSlot} whose effective occupant is requested
     * @return the effective occupant or {@code null} if none
     */
    @SuppressWarnings("unchecked")
    default <T> OutputSlot<T> getEffectiveOccupant(int inputIndex) {
        return this.getOutermostInputSlot((InputSlot<T>) this.getInput(inputIndex)).getOccupant();
    }

    /**
     * Retrieves the effective occupant of the given {@link InputSlot}, i.e., the {@link OutputSlot} that is
     * either connected to the given or an outer-more, mapped {@link InputSlot}.
     *
     * @param input whose effective occupant is requested
     * @return the effective occupant or {@code null} if none
     */
    default <T> OutputSlot<T> getEffectiveOccupant(InputSlot<T> input) {
        return this.getOutermostInputSlot(input).getOccupant();
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

    default boolean isOwnerOf(Slot<?> slot) {
        return slot.getOwner() == this;
    }

    /**
     * Declare forward rules. Execution engines may take the chance to optimize the executed plans by having
     * forwarded data by-pass this instance. However, note the specific semantics of a forward rule: If an
     * {@link Operator} serves an {@link OutputSlot} that is involved in a foward rule, it will do so by forwarding.
     * If the {@link OutputSlot} is not served, then the forwarding does not apply.
     *
     * @return {@link OutputSlot}s to which this instance forwards the given {@code input}.
     * @see #isReading(InputSlot)
     */
    default Collection<OutputSlot<?>> getForwards(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        return Collections.emptyList();
    }

    /**
     * Checks whether this instance is not connected to any other instance via its {@link Slot}s. This is a typical
     * property of instances used for {@link org.qcri.rheem.core.optimizer.channels.ChannelConversion}s.
     *
     * @return whether this instance is unconnected
     */
    default boolean isUnconnected() {
        for (InputSlot<?> inputSlot : this.getAllInputs()) {
            if (inputSlot.getOccupant() != null) return false;
        }
        for (OutputSlot<?> outputSlot : this.getAllOutputs()) {
            if (!outputSlot.getOccupiedSlots().isEmpty()) return false;
        }
        return true;
    }

    /**
     * Tells whether the given {@code input} is read by this operator. If not, the optimizer can make use of this
     * insight.
     *
     * @see #getForwards(InputSlot)
     */
    default boolean isReading(InputSlot<?> input) {
        return true;
    }

    /**
     * @return whether this {@code input} is used to close a feedback loop (i.e., a flow graph cycle)
     */
    default boolean isFeedbackInput(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        return this.isLoopHead() && ((LoopHeadOperator) this).getLoopBodyInputs().contains(input);
    }

    /**
     * @return whether this {@code output} is used to start a feedback loop (i.e., a flow graph cycle)
     */
    default boolean isFeedforwardOutput(OutputSlot<?> ouput) {
        assert this.isOwnerOf(ouput);
        return this.isLoopHead() && ((LoopHeadOperator) this).getLoopBodyOutputs().contains(ouput);
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

    default boolean isLoopSubplan() {
        return this instanceof LoopSubplan;
    }

    default boolean isAlternative() {
        return this instanceof OperatorAlternative;
    }

    default boolean isExecutionOperator() {
        return this instanceof ExecutionOperator;
    }

    /**
     * Identify this instance as the head of a loop. It is the only kind of {@link Operator} that can cause
     * data flow cycles.
     *
     * @return whether this instance is the head of a loop
     */
    default boolean isLoopHead() {
        return this instanceof LoopHeadOperator;
    }

    /**
     * @return whether this is an elementary operator
     */
    default boolean isElementary() {
        return true;
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
        return container == null ? null : container.toOperator();
    }

    OperatorContainer getContainer();

    /**
     * Operators can be nested in other operators, e.g., in a {@link Subplan} or a {@link OperatorAlternative.Alternative}.
     *
     * @param newContainer the new container of this operator or {@code null} to declare it top-level
     */
    void setContainer(OperatorContainer newContainer);

    /**
     * @return the innermost {@link LoopSubplan} containing this instance
     */
    default LoopSubplan getInnermostLoop() {
        final CompositeOperator parent = this.getParent();
        if (parent == null) {
            return null;
        } else if (parent.isLoopSubplan()) {
            return (LoopSubplan) parent;
        } else {
            return parent.getInnermostLoop();
        }
    }

    /**
     * @return the stack of nested {@link LoopSubplan}s of this instance, from inside to outside
     */
    default LinkedList<LoopSubplan> getLoopStack() {
        LinkedList<LoopSubplan> loopStack = new LinkedList<>();
        LoopSubplan nextLoop = this.getInnermostLoop();
        while (nextLoop != null) {
            loopStack.addLast(nextLoop);
            nextLoop = nextLoop.getInnermostLoop();
        }
        return loopStack;
    }

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
     * Provide a {@link CardinalityPusher} for the {@link Operator}.
     *
     * @param configuration if the {@link CardinalityPusher} depends on further ones, use this to obtain the latter
     * @return the {@link CardinalityPusher}
     */
    default CardinalityPusher getCardinalityPusher(final Configuration configuration) {
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
     */
    void addTargetPlatform(Platform platform);

    /**
     * Convenience version of {@link Operator#propagateOutputCardinality(int, OptimizationContext.OperatorContext)},
     * where the adjacent {@link InputSlot}s reside in the same {@link OptimizationContext} as the {@code operatorContext}.
     */
    default void propagateOutputCardinality(int outputIndex, OptimizationContext.OperatorContext operatorContext) {
        this.propagateOutputCardinality(outputIndex, operatorContext, operatorContext.getOptimizationContext());
    }

    /**
     * Propagates the {@link CardinalityEstimate} of an {@link OutputSlot} within the {@code operatorContext} to
     * <ul>
     * <li>fed {@link InputSlot}s (which in turn are asked to propagate) and</li>
     * <li><b>inner</b>, mapped {@link OutputSlot}s.</li>
     * </ul>
     *
     * @param outputIndex     of the {@link OutputSlot}
     * @param operatorContext holds the {@link CardinalityEstimate} to be propagated
     * @param targetContext   to which the {@link CardinalityEstimate}s should be propagated
     */
    void propagateOutputCardinality(int outputIndex,
                                    OptimizationContext.OperatorContext operatorContext,
                                    OptimizationContext targetContext);

    /**
     * Propagates the {@link CardinalityEstimate} of an {@link InputSlot} within the {@code operatorContext}
     * to <b>inner</b>, mapped {@link InputSlot}s.
     *
     * @param inputIndex      of the {@link InputSlot}
     * @param operatorContext holds the {@link CardinalityEstimate} to be propagated
     */
    void propagateInputCardinality(int inputIndex,
                                   OptimizationContext.OperatorContext operatorContext);

    /**
     * Collect all inner {@link OutputSlot}s that are mapped to the given {@link OutputSlot}.
     */
    <T> Set<OutputSlot<T>> collectMappedOutputSlots(OutputSlot<T> output);

    /**
     * Collect all inner {@link InputSlot}s that are mapped to the given {@link InputSlot}.
     */
    <T> Set<InputSlot<T>> collectMappedInputSlots(InputSlot<T> input);

    /**
     * Provides an instance's name.
     *
     * @return the name of this instance or {@code null} if none
     */
    String getName();

    /**
     * Provide a name for this instance.
     *
     * @param name the name
     */
    void setName(String name);

    /**
     * Collects all fields of this instance that have a {@link EstimationContextProperty} annotation.
     *
     * @return the fields
     */
    default Collection<String> getEstimationContextProperties() {
        Set<String> properties = new HashSet<>(2);
        Queue<Class<?>> classQueue = new LinkedList<>();
        classQueue.add(this.getClass());
        while (!classQueue.isEmpty()) {
            final Class<?> cls = classQueue.poll();
            if (cls.getSuperclass() != null) classQueue.add(cls.getSuperclass());
            for (Field declaredField : cls.getDeclaredFields()) {
                final EstimationContextProperty annotation = declaredField.getDeclaredAnnotation(EstimationContextProperty.class);
                if (annotation != null) {
                    properties.add(declaredField.getName());
                }
            }
        }
        return properties;
    }

}

