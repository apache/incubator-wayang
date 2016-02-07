package org.qcri.rheem.core.plan.executionplan;

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


}
