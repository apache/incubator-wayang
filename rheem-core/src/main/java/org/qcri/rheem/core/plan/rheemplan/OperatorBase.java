package org.qcri.rheem.core.plan.rheemplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.platform.Platform;

import java.util.Arrays;
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
    public void addBroadcastInput(InputSlot<?> broadcastInput) {
        Validate.isTrue(this.isSupportingBroadcastInputs(), "%s does not support broadcast inputs.", this);
        Validate.isTrue(
                Arrays.stream(this.getAllInputs()).noneMatch(input -> input.getName().equals(broadcastInput.getName())),
                "The name %s is already taken in %s.", broadcastInput.getName(), this
        );
        Validate.isTrue(broadcastInput.isBroadcast());
        final InputSlot<?>[] newInputs = new InputSlot<?>[this.getNumInputs() + 1];
        System.arraycopy(this.getAllInputs(), 0, newInputs, 0, this.getNumInputs());
        newInputs[this.getNumInputs()] = broadcastInput;
        this.inputSlots = newInputs;
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
        return String.format("%s[%d->%d, id=%x]",
                this.getClass().getSimpleName(),
                this.getNumInputs(),
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
}
