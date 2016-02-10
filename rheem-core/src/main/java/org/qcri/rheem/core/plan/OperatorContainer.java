package org.qcri.rheem.core.plan;

import java.util.Collection;

/**
 * This is not an {@link Operator} in its own right. However, it contains a set of operators and can redirect
 * to those.
 */
public interface OperatorContainer {

    /**
     * Provide the {@link SlotMapping} that translates between the contained {@link Operator}s and the containing
     * {@link CompositeOperator} as defined by {@link #toOperator()}.
     *
     * @return the above explained {@link SlotMapping}
     */
    SlotMapping getSlotMapping();

    /**
     * Enter this container. This container's {@link CompositeOperator} needs to be a source.
     *
     * @return the sink operator within this container
     */
    Operator getSource();

    /**
     * Enter the encased plan by following an {@code inputSlot} of the encasing {@link CompositeOperator}.
     *
     * @param inputSlot an {@link InputSlot} of the encasing {@link CompositeOperator}
     * @return the {@link InputSlot}s within the encased plan that are connected to the given {@code inputSlot}
     */
    <T> Collection<InputSlot<T>> followInput(InputSlot<T> inputSlot);

    /**
     * Enter this container. This container's {@link CompositeOperator} needs to be a sink.
     *
     * @return the sink operator within this subplan
     */
    Operator getSink();

    /**
     * Enter the encased plan by following an {@code outputSlot} of the encasing {@link CompositeOperator}.
     *
     * @param outputSlot an {@link OutputSlot} of the encasing {@link CompositeOperator}
     * @return the {@link OutputSlot} within the encased plan that are connected to the given {@code outputSlot}
     */
    <T> OutputSlot<T> traceOutput(OutputSlot<T> outputSlot);

    /**
     * @return the {@link CompositeOperator} that corresponds to this instance
     */
    CompositeOperator toOperator();


    /**
     * Return the {@link InputSlot} that represents the given {@code inputSlot}. NB: This method assumes the given
     * {@code inputSlot} has no occupent (see {@link InputSlot#getOccupant()}).
     *
     * @param inputSlot the {@link InputSlot} of a contained {@link Operator} that is to be resolved
     * @return the {@link InputSlot} that represents the given {@code inputSlot} or {@code null} if there is none
     */
    <T> InputSlot<T> traceInput(InputSlot<T> inputSlot);

    /**
     * Exit the encased plan by following an {@code outputSlot} of an encased, terminal {@link Operator}.
     *
     * @param outputSlot the {@link OutputSlot} to follow
     * @return the {@link OutputSlot}s of the encasing {@link CompositeOperator} (see {@link #toOperator()}) that
     * represent the given {@code outputSlot} or {@code null} if none
     */
    <T> Collection<OutputSlot<T>> followOutput(OutputSlot<T> outputSlot);
}
