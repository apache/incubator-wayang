package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityPusher;

import java.util.Collection;

/**
 * Head of a {@link LoopSubplan}.
 */
public interface LoopHeadOperator extends Operator {


    /**
     * If this instance is the head of a loop, then return these {@link OutputSlot}s that go into the loop body (as
     * opposed to the {@link OutputSlot}s that form the final result of the iteration).
     *
     * @return the loop body-bound {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getLoopBodyOutputs();

    /**
     * If this instance is the head of a loop, then return these {@link OutputSlot}s that form the final result of
     * the iteration.
     *
     * @return the loop-terminal {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getFinalLoopOutputs();

    /**
     * If this instance is the head of a loop, then return these {@link InputSlot}s that are fed from the loop body (as
     * opposed to the {@link InputSlot}s that initialize the loop).
     *
     * @return the loop body-bound {@link InputSlot}s
     */
    Collection<InputSlot<?>> getLoopBodyInputs();

    /**
     * If this instance is the head of a loop, then return these {@link InputSlot}s that initialize the loop.
     *
     * @return the initialization {@link InputSlot}s
     */
    Collection<InputSlot<?>> getLoopInitializationInputs();

    /**
     * Retrieve those {@link InputSlot}s that are required to evaluate the loop condition.
     *
     * @return the condition {@link InputSlot}s
     */
    Collection<InputSlot<?>> getConditionInputSlots();

    /**
     * Retrieve those {@link OutputSlot}s that forward the {@link #getConditionInputSlots()}.
     *
     * @return the condition {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getConditionOutputSlots();



    /**
     * @return a number of expected iterations; not necessarily the actual value
     */
    int getNumExpectedIterations();

    /**
     * Get the {@link CardinalityPusher} implementation for the intermediate iterations.
     */
    @Override
    default CardinalityPusher getCardinalityPusher(final Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * Get the {@link CardinalityPusher} implementation for the initial iteration.
     */
    default CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopInitializationInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * Get the {@link CardinalityPusher} implementation for the final iteration.
     */
    default CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getFinalLoopOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * <i>Optional operation. Only {@link ExecutionOperator}s need to implement this method.</i>
     * @return the current {@link State} of this instance
     */
    default State getState() {
        assert !this.isExecutionOperator();
        throw new UnsupportedOperationException();
    }

    /**
     * <i>Optional operation. Only {@link ExecutionOperator}s need to implement this method.</i> Sets the {@link State} of this instance.
     */
    default void setState(State state) {
        assert !this.isExecutionOperator();
        throw new UnsupportedOperationException();
    }

    /**
     * {@link LoopHeadOperator}s can be stateful because they might be executed mulitple times. This {@code enum}
     * expresses this state.
     */
    enum State {
        /**
         * The {@link LoopHeadOperator} has not been executed yet.
         */
        NOT_STARTED,

        /**
         * The {@link LoopHeadOperator} has been executed. However, the last execution was not the ultimate one.
         */
        RUNNING,

        /**
         * The {@link LoopHeadOperator} has been executed ultimately.
         */
        FINISHED
    }
}
