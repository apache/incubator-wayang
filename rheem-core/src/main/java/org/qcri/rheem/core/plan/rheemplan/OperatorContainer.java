package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

import java.util.Collection;
import java.util.Collections;

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
     * @return the source operator within this container
     */
    Operator getSource();

    /**
     * Set the source. This container's {@link CompositeOperator} needs to be a source itself.
     *
     * @param innerSource the source operator within this container
     */
    void setSource(Operator innerSource);

    /**
     * Enter the encased plan by following an {@code inputSlot} of the encasing {@link CompositeOperator}.
     *
     * @param inputSlot an {@link InputSlot} of the encasing {@link CompositeOperator}
     * @return the {@link InputSlot}s within the encased plan that are connected to the given {@code inputSlot}
     */
    <T> Collection<InputSlot<T>> followInput(InputSlot<T> inputSlot);

    /**
     * @see #followInput(InputSlot)
     */
    @SuppressWarnings("unchecked")
    default Collection<InputSlot<?>> followInputUnchecked(InputSlot<?> inputSlot) {
        return (Collection<InputSlot<?>>) (Collection) this.followInput(inputSlot);
    }

    /**
     * Enter this container. This container's {@link CompositeOperator} needs to be a sink.
     *
     * @return the sink operator within this subplan
     */
    Operator getSink();

    /**
     * Set the sink. This container's {@link CompositeOperator} needs to be a sink itself.
     *
     * @param innerSink the sink operator within this container
     */
    void setSink(Operator innerSink);

    /**
     * @return whether this container corresponds to a sink
     */
    default boolean isSink() {
        return this.toOperator().isSink();
    }

    /**
     * Enter the encased plan by following an {@code outputSlot} of the encasing {@link CompositeOperator}.
     *
     * @param outputSlot an {@link OutputSlot} of the encasing {@link CompositeOperator}
     * @return the {@link OutputSlot} within the encased plan that are connected to the given {@code outputSlot}
     */
    <T> OutputSlot<T> traceOutput(OutputSlot<T> outputSlot);

    /**
     * @return the {@link CompositeOperator} that corresponds to this instance, <i>not</i> any of the contained
     * {@link Operator}s
     */
    CompositeOperator toOperator();


    /**
     * Return the {@link InputSlot} that represents the given {@code inputSlot}. NB: This method assumes the given
     * {@code inputSlot} has no occupant (see {@link InputSlot#getOccupant()}).
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

    /**
     * Propagates the {@link CardinalityEstimate} of the given {@link InputSlot} to inner, mapped {@link InputSlot}s
     *
     * @see Operator#propagateInputCardinality(int, OptimizationContext.OperatorContext)
     */
    default void propagateInputCardinality(int inputIndex,
                                           OptimizationContext.OperatorContext operatorContext) {
        final CompositeOperator compositeOperator = this.toOperator();
        assert operatorContext.getOperator() == compositeOperator;
        assert 0 <= inputIndex && inputIndex <= compositeOperator.getNumInputs();
        final Collection<? extends InputSlot<?>> innerInputs = this.followInput(this.toOperator().getInput(inputIndex));

        for (InputSlot<?> innerInput : innerInputs) {
            Collection<OptimizationContext> innerOptimizationCtxs =
                    this.getInnerInputOptimizationContext(innerInput, operatorContext.getOptimizationContext());
            for (OptimizationContext innerOptimizationCtx : innerOptimizationCtxs) {
                // Identify the appropriate OperatorContext.
                OptimizationContext.OperatorContext innerOperatorCtx = innerOptimizationCtx.getOperatorContext(innerInput.getOwner());

                // Update the CardinalityEstimate.
                final CardinalityEstimate cardinality = operatorContext.getInputCardinality(inputIndex);
                innerOperatorCtx.setInputCardinality(innerInput.getIndex(), cardinality);

                // Continue the propagation.
                innerInput.getOwner().propagateInputCardinality(innerInput.getIndex(), innerOperatorCtx);
            }
        }
    }

    /**
     * Propagates the {@link CardinalityEstimate} of the given {@link OutputSlot} to inner, mapped {@link OutputSlot}s
     *
     * @see Operator#propagateOutputCardinality(int, OptimizationContext.OperatorContext)
     */
    default void propagateOutputCardinality(int outputIndex, OptimizationContext.OperatorContext operatorCtx) {
        final CompositeOperator compositeOperator = this.toOperator();
        assert operatorCtx.getOperator() == compositeOperator;
        final OutputSlot<?> innerOutput = this.traceOutput(compositeOperator.getOutput(outputIndex));

        if (innerOutput != null) {
            // Identify the appropriate OperatorContext.
            OptimizationContext innerOptimizationCtx = operatorCtx.getOptimizationContext();
            OptimizationContext.OperatorContext innerOperatorCtx = innerOptimizationCtx.getOperatorContext(innerOutput.getOwner());

            // Update the CardinalityEstimate.
            final CardinalityEstimate cardinality = operatorCtx.getOutputCardinality(outputIndex);
            innerOperatorCtx.setOutputCardinality(innerOutput.getIndex(), cardinality);

            // Continue the propagation.
            innerOutput.getOwner().propagateOutputCardinality(innerOutput.getIndex(), innerOperatorCtx);
        }
    }

    /**
     * Retrieve those {@link OptimizationContext}s that represent the state when this instance is entered.
     *
     * @param innerInput               the inner {@link InputSlot} whose {@link OptimizationContext}s are requested
     * @param outerOptimizationContext the {@link OptimizationContext} in that this instance resides
     * @return the inner {@link OptimizationContext}s
     */
    default Collection<OptimizationContext> getInnerInputOptimizationContext(
            InputSlot<?> innerInput,
            OptimizationContext outerOptimizationContext) {
        // Usually the same.
        return Collections.singleton(outerOptimizationContext);
    }

    /**
     * Retrieve that {@link OptimizationContext} that represents the state when this instance is exited.
     *
     * @param outerOptimizationContext the {@link OptimizationContext} in that this instance resides
     * @return the inner {@link OptimizationContext}
     */
    default OptimizationContext getInnerOutputOptimizationContext(OptimizationContext outerOptimizationContext) {
        // Usually the same.
        return outerOptimizationContext;
    }
}
