package org.qcri.rheem.core.plan.rheemplan;

import java.util.List;

/**
 * Wraps a loop of {@link Operator}s.
 *
 * @see LoopIsolator
 */
public class LoopSubplan extends Subplan {

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     */
    public static LoopSubplan wrap(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        return new LoopSubplan(inputs, outputs, container);
    }

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     *
     * @param inputs
     * @param outputs
     * @param container
     * @see #wrap(Operator, Operator)
     * @see #wrap(List, List, OperatorContainer)
     */
    private LoopSubplan(List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs, OperatorContainer container) {
        super(inputs, outputs, container);
    }
}
