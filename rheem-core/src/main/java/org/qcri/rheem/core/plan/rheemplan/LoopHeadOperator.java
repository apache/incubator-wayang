package org.qcri.rheem.core.plan.rheemplan;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityPusher;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityPusher;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;

import java.util.*;

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

    /**
     * Create output {@link ChannelInstance}s for this instance.
     *
     * @param task                    the {@link ExecutionTask} in which this instance is being wrapped
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for this instance
     * @param inputChannelInstances   the input {@link ChannelInstance}s for the {@code task}
     * @return
     */
    default ChannelInstance[] createOutputChannelInstances(Executor executor,
                                                             ExecutionTask task,
                                                             OptimizationContext.OperatorContext producerOperatorContext,
                                                             List<ChannelInstance> inputChannelInstances) {

        assert task.getOperator() == this;

        ChannelInstance[] channelInstances = new ChannelInstance[task.getNumOuputChannels()];
        final Collection<OutputSlot<?>> conditionOutputs = this.getConditionOutputSlots();
        @SuppressWarnings("unchecked")
        final Collection<OutputSlot<?>> regularOutputs  = new ArrayList(Arrays.asList(this.getAllOutputs()));
        regularOutputs.removeAll(conditionOutputs);

        final Collection<InputSlot<?>> conditionInputs = this.getConditionInputSlots();
        @SuppressWarnings("unchecked")
        final Collection<InputSlot<?>> regularInputs  = new ArrayList(Arrays.asList(this.getAllInputs()));
        regularInputs.removeAll(conditionInputs);

        // Create ChannelInstances for the condition OutputSlots.
        for (OutputSlot<?> output : conditionOutputs) {
            final Channel outputChannel = task.getOutputChannel(output.getIndex());
            final ChannelInstance outputChannelInstance = outputChannel.createInstance(executor, producerOperatorContext, output.getIndex());
            channelInstances[output.getIndex()] = outputChannelInstance;
            // Link only with condition InputSlots.
            for (InputSlot<?> input : conditionInputs) {
                ChannelInstance inputChannelInstance = inputChannelInstances.get(input.getIndex());
                if (inputChannelInstance != null) {
                    outputChannelInstance.addPredecessor(inputChannelInstance);
                }
            }
        }

        // Create ChannelInstances for the regular OutputSlots.
        for (OutputSlot<?> output : regularOutputs) {
            final Channel outputChannel = task.getOutputChannel(output.getIndex());
            // We assume that the ChannelInstances are just forwarded, so don't schedule the execution of this instance.
            final ChannelInstance outputChannelInstance = outputChannel.createInstance(executor, null, -1);
            channelInstances[output.getIndex()] = outputChannelInstance;
            // Link only with regular InputSlots.
            for (InputSlot<?> input : regularInputs) {
                ChannelInstance inputChannelInstance = inputChannelInstances.get(input.getIndex());
                if (inputChannelInstance != null) {
                    outputChannelInstance.addPredecessor(inputChannelInstance);
                }
            }
        }

        return channelInstances;
    }

}
