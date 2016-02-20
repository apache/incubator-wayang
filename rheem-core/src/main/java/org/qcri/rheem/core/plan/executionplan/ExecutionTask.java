package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Platform;

/**
 * Serves as an adapter to include {@link ExecutionOperator}s, which are usually parts of {@link RheemPlan}s, in
 * {@link ExecutionPlan}s.
 */
public class ExecutionTask {

    /**
     * {@link ExecutionOperator} that is being adapted by this instance.
     */
    private final ExecutionOperator operator;

    /**
     * {@link Channel}s that this instance reads from. Align with {@link #operator}'s {@link InputSlot}s
     *
     * @see ExecutionOperator#getAllInputs()
     */
    private final Channel[] inputChannels;

    /**
     * {@link Channel}s that this instance writes to. Align with {@link #operator}'s {@link OutputSlot}s
     *
     * @see ExecutionOperator#getAllOutputs()
     */
    private final Channel[] outputChannels;

    /**
     * Captures if this instance is part of a {@link ExecutionStage} and of which.
     */
    private ExecutionStage stage;

    public ExecutionTask(ExecutionOperator operator) {
        this(operator, operator.getNumInputs(), operator.getNumOutputs());
    }

    public ExecutionTask(ExecutionOperator operator, int numInputChannels, int numOutputChannels) {
        this.operator = operator;
        this.inputChannels = new Channel[numInputChannels];
        this.outputChannels = new Channel[numOutputChannels];
    }

    public ExecutionOperator getOperator() {
        return this.operator;
    }

    public Channel[] getInputChannels() {
        return this.inputChannels;
    }

    public int getNumInputChannels() {
        return this.getInputChannels().length;
    }

    public Channel getInputChannel(int index) {
        return this.getInputChannels()[index];
    }

    /**
     * Sets an input {@link Channel} for this instance. Consider using {@link Channel#addConsumer(ExecutionTask, int)}
     * instead.
     */
    void setInputChannel(int index, Channel channel) {
        assert channel == null || this.getInputChannel(index) == null;
        this.getInputChannels()[index] = channel;
    }

    /**
     * Exchanges input {@link Channel}. Will also update the {@link Channel}'s consumers appropriately.
     */
    public void exchangeInputChannel(Channel currentChannel, Channel newChannel) {
        for (int inputIndex = 0; inputIndex < this.getNumInputChannels(); inputIndex++) {
            if (this.getInputChannel(inputIndex) == currentChannel) {
                currentChannel.getConsumers().remove(this);
                this.setInputChannel(inputIndex, null);
                newChannel.addConsumer(this, inputIndex);
                return;
            }
        }
        throw new IllegalArgumentException(String.format("%s is not an input of %s.", currentChannel, this));
    }

    public Channel[] getOutputChannels() {
        return this.outputChannels;
    }

    public int getNumOuputChannels() {
        return this.getOutputChannels().length;
    }

    public Channel getOutputChannel(int index) {
        return this.getOutputChannels()[index];
    }

    /**
     * Sets an output {@link Channel} for this instance. Consider using {@link Channel#Channel(ExecutionTask, int)}
     * and derivatives instead.
     */
    void setOutputChannel(int index, Channel channel) {
        Validate.isTrue(this.getOutputChannel(index) == null, "Output channel %d of %s is already set to %s.",
                index, this, this.getOutputChannel(index));
        this.getOutputChannels()[index] = channel;
    }

    public ExecutionStage getStage() {
        return this.stage;
    }

    public void setStage(ExecutionStage stage) {
        this.stage = stage;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.operator + ']';
    }

    /**
     * Returns the {@link OutputSlot} of the {@link ExecutionOperator} that is associated to the given {@link Channel}.
     *
     * @return the {@link OutputSlot} or {@code null} if none
     */
    public OutputSlot<?> getOutputSlotFor(Channel channel) {
        // Simple implementation: linear search.
        for (int outputIndex = 0; outputIndex < this.getNumOuputChannels(); outputIndex++) {
            if (this.getOutputChannel(outputIndex) == channel) {
                return outputIndex < this.getOperator().getNumOutputs() ?
                        this.getOperator().getOutput(outputIndex) :
                        null;
            }
        }
        throw new IllegalArgumentException(String.format("%s does not belong to %s.", channel, this));
    }

    /**
     * Returns the {@link InputSlot} of the {@link ExecutionOperator} that is associated to the given {@link Channel}.
     *
     * @return the {@link InputSlot} or {@code null} if none
     */
    public InputSlot<?> getInputSlotFor(Channel channel) {
        // Simple implementation: linear search.
        for (int inputIndex = 0; inputIndex < this.getNumInputChannels(); inputIndex++) {
            if (this.getInputChannel(inputIndex) == channel) {
                return inputIndex < this.getOperator().getNumInputs() ?
                        this.getOperator().getInput(inputIndex) :
                        null;
            }
        }
        throw new IllegalArgumentException(String.format("%s does not belong to %s.", channel, this));
    }

    /**
     * @return the {@link Platform} for the encased {@link ExecutionOperator}
     */
    public Platform getPlatform() {
        return this.operator.getPlatform();
    }
}
