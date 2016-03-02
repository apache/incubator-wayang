package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class for the implementation of the {@link Operator} interface.
 */
public abstract class OperatorBase implements Operator {

    private final boolean isSupportingBroadcastInputs;

    private OperatorContainer container;

    private int epoch = FIRST_EPOCH;

    protected InputSlot<?>[] inputSlots;

    protected final OutputSlot<?>[] outputSlots;

    private final Set<Platform> targetPlatforms = new HashSet<>(0);

    private ExecutionOperator original;

    /**
     * Optional name. Helpful for debugging.
     */
    private String name;

    /**
     * Can assign a {@link TimeEstimate} to {@link ExecutionOperator}s.
     */
    private TimeEstimate timeEstimate;

    public OperatorBase(InputSlot<?>[] inputSlots, OutputSlot<?>[] outputSlots, boolean isSupportingBroadcastInputs,
                        OperatorContainer container) {
        this.container = container;
        this.isSupportingBroadcastInputs = isSupportingBroadcastInputs;
        this.inputSlots = inputSlots;
        this.outputSlots = outputSlots;
    }

    public OperatorBase(int numInputSlots, int numOutputSlots, boolean isSupportingBroadcastInputs, OperatorContainer container) {
        this(new InputSlot[numInputSlots], new OutputSlot[numOutputSlots], isSupportingBroadcastInputs, container);
    }

    @Override
    public InputSlot<?>[] getAllInputs() {
        return this.inputSlots;
    }

    @Override
    public OutputSlot<?>[] getAllOutputs() {
        return this.outputSlots;
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return this.isSupportingBroadcastInputs;
    }

    @Override
    public int addBroadcastInput(InputSlot<?> broadcastInput) {
        Validate.isTrue(this.isSupportingBroadcastInputs(), "%s does not support broadcast inputs.", this);
        Validate.isTrue(
                Arrays.stream(this.getAllInputs()).noneMatch(input -> input.getName().equals(broadcastInput.getName())),
                "The name %s is already taken in %s.", broadcastInput.getName(), this
        );
        Validate.isTrue(broadcastInput.isBroadcast());
        final int oldNumInputSlots = this.getNumInputs();
        final InputSlot<?>[] newInputs = new InputSlot<?>[oldNumInputSlots + 1];
        System.arraycopy(this.getAllInputs(), 0, newInputs, 0, oldNumInputSlots);
        newInputs[oldNumInputSlots] = broadcastInput;
        this.inputSlots = newInputs;
        return oldNumInputSlots;
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return null;
    }

    @Override
    public OperatorContainer getContainer() {
        return this.container;
    }

    @Override
    public void setContainer(OperatorContainer newContainer) {
        final CompositeOperator currentParent = this.getParent();
        if (currentParent != null) {
            currentParent.replace(this, newContainer.toOperator());
        }
        this.container = newContainer;
    }

    @Override
    public int getEpoch() {
        return this.epoch;
    }

    @Override
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Convenience method to set the epoch.
     */
    public Operator at(int epoch) {
        this.setEpoch(epoch);
        return this;
    }

    @Override
    public String toString() {
        if (this.name != null) {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.name);
        }
        long numBroadcasts = Arrays.stream(this.getAllInputs()).filter(InputSlot::isBroadcast).count();
        return String.format("%s[%d%s->%d, id=%x]",
                this.getClass().getSimpleName(),
                this.getNumInputs() - numBroadcasts,
                numBroadcasts == 0 ? "" : "+" + numBroadcasts,
                this.getNumOutputs(),
//                this.getParent() == null ? "top-level" : "nested",
                this.hashCode());
    }

    @Override
    public Set<Platform> getTargetPlatforms() {
        return this.targetPlatforms;
    }

    @Override
    public void addTargetPlatform(Platform platform) {
        this.targetPlatforms.add(platform);
    }

    public TimeEstimate getTimeEstimate() {
        if (!this.isExecutionOperator()) {
            throw new IllegalStateException("Cannot retrieve time estimate for non-execution operator " + this);
        }
        return this.timeEstimate;
    }

    public void setTimeEstimate(TimeEstimate timeEstimate) {
        if (!this.isExecutionOperator()) {
            throw new IllegalStateException("Cannot set time estimate for non-execution operator " + this);
        }
        this.timeEstimate = timeEstimate;
    }

    @Override
    public void propagateOutputCardinality(int outputIndex,
                                           OptimizationContext.OperatorContext operatorContext,
                                           OptimizationContext targetContext) {
        assert operatorContext.getOperator() == this;

        // Identify the cardinality.
        final OutputSlot<?> output = this.getOutput(outputIndex);
        final CardinalityEstimate cardinality = operatorContext.getOutputCardinality(outputIndex);

        // Propagate to the InputSlots.
        for (InputSlot<?> inputSlot : output.getOccupiedSlots()) {
            // Find the adjacent OperatorContext corresponding to the inputSlot.
            final int inputIndex = inputSlot.getIndex();
            final Operator adjacentOperator = inputSlot.getOwner();
            final OptimizationContext.OperatorContext adjacentOperatorCtx = targetContext.getOperatorContext(adjacentOperator);
            assert adjacentOperatorCtx != null : String.format("Missing OperatorContext for %s.", adjacentOperator);

            // Update the adjacent OperatorContext.
            adjacentOperatorCtx.setInputCardinality(inputIndex, cardinality);
            adjacentOperator.propagateInputCardinality(inputIndex, adjacentOperatorCtx);
        }
    }

    @Override
    public void propagateInputCardinality(int inputIndex, OptimizationContext.OperatorContext operatorContext) {
        // Nothing to do for elementary operators.
    }

    @Override
    public <T> Set<OutputSlot<T>> collectMappedOutputSlots(OutputSlot<T> output) {
        // Default implementation for elementary instances.
        assert this.isElementary();
        assert output.getOwner() == this;
        return Collections.singleton(output);
    }

    @Override
    public <T> Set<InputSlot<T>> collectMappedInputSlots(InputSlot<T> input) {
        throw new RuntimeException("Implement me.");
    }

    /**
     * @see ExecutionOperator#copy()
     */
    public ExecutionOperator copy() {
        assert this.isExecutionOperator();
        ExecutionOperator copy = this.createCopy();
        ((OperatorBase) copy).original = this.getOriginal();
        return copy;
    }

    protected ExecutionOperator createCopy() {
        throw new RuntimeException("Not implemented.");
    }

    /**
     * @see ExecutionOperator#getOriginal()
     */
    public ExecutionOperator getOriginal() {
        assert this.isExecutionOperator();
        return this.original == null ? (ExecutionOperator) this : this.original;
    }

    public String getName() {
        return this.name;
    }

    @SuppressWarnings("unchecked")
    public <Self extends OperatorBase> Self setName(String name) {
        this.name = name;
        return (Self) this;
    }
}
