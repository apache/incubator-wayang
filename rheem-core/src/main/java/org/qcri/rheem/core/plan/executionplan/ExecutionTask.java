package org.qcri.rheem.core.plan.executionplan;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

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

    public ExecutionTask(ExecutionOperator operator) {
        this.operator = operator;
        this.inputChannels = new Channel[operator.getNumInputs()];
        this.outputChannels = new Channel[operator.getNumOutputs()];
    }

    public ExecutionOperator getOperator() {
        return this.operator;
    }

    public Channel[] getInputChannels() {
        return this.inputChannels;
    }

    public Channel getInputChannel(int index) {
        return this.getInputChannels()[index];
    }

    void setInputChannel(int index, Channel channel) {
        Validate.isTrue(this.getInputChannel(index) == null);
        this.getInputChannels()[index] = channel;
    }

    public Channel[] getOutputChannels() {
        return this.outputChannels;
    }

    public Channel getOutputChannel(int index) {
        return this.getOutputChannels()[index];
    }

    void setOutputChannel(int index, Channel channel) {
        Validate.isTrue(this.getOutputChannel(index) == null, "Output channel %d of %s is already set to %s.",
                index, this, this.getOutputChannel(index));
        this.getOutputChannels()[index] = channel;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.operator + ']';
    }
}
