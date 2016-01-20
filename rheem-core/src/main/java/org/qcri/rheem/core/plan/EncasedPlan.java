package org.qcri.rheem.core.plan;

import java.util.Collection;

/**
 * This is not an {@link Operator} in its own right. However, it contains a set of operators and can redirect
 * to those.
 */
public interface EncasedPlan {

    Operator getSource();

    <T> Collection<InputSlot<T>> followInput(InputSlot<T> inputSlot);

    /**
     * Enter this subplan. This subplan needs to be a sink.
     *
     * @return the sink operator within this subplan
     */
    Operator getSink();

    /**
     * Enter the encased plan by following one of its output slots.
     *
     * @param outputSlot an output slot of this subplan
     * @return the output within the subplan that is connected to the given output slot
     */
    <T> OutputSlot<T> traceOutput(OutputSlot<T> outputSlot);

}
