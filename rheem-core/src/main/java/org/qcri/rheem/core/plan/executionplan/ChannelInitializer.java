package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Tuple;

/**
 * Sets up the usage of {@link Channel} in an {@link ExecutionPlan} for a given {@link Platform}.
 */
public interface ChannelInitializer {

//    /**
//     * <i>Optional operation.</i> Implements the {@link Channel} on the {@code index}-th output of the given
//     * {@link ExecutionTask}, thereby adding potentially further {@link ExecutionTask}s required for using the
//     * {@link Channel}. However, if it encounters already set-up {@link Channel}s, the implementation is free to
//     * reuse them if appropriate or even necessary.
//     *
//     * @param descriptor    describes the {@link Channel} to be created
//     * @param executionTask that should output to the new {@link Channel}
//     * @param index         the output index of the {@code executionTask} that should feed the {@link Channel}
//     * @return the newly created and set-up or reused {@link Channel}
//     */
//    Channel setUpOutput(ChannelDescriptor descriptor, ExecutionTask executionTask, int index);

    /**
     * Todo.
     *
     * @param optimizationContext provides estimates and accepts new {@link Operator}s
     * @return {@link Channel} that is directly output by the {@code outputSlot} and the {@link Channel} that was
     * actually requested; both are interlinked.
     */
    Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext);

    /**
     * Todo.
     *
     * @param optimizationContext provides estimates and accepts new {@link Operator}s
     * @return {@link Channel} that is directly output by the {@code outputSlot} and the {@link Channel} that was
     * actually requested; both are interlinked.
     */
    Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext);

//    /**
//     * <i>Optional operation.</i> Implements the {@link Channel} on the {@code index}-th output of the given
//     * {@link ExecutionTask}, thereby adding potentially further {@link ExecutionTask}s required for using the
//     * {@link Channel}. It may reuse existing {@link ExecutionTask}s and {@link Channel}s when appropriate or
//     * necessary.
//     *
//     * @param channel       the {@link Channel} to be consumed as input
//     * @param executionTask that should output to the new {@link Channel}
//     * @param index         the output index of the {@code executionTask} that should feed the {@link Channel}
//     */
//    void setUpInput(Channel channel, ExecutionTask executionTask, int index);

    /**
     * Erases the type variable from this instance.
     *
     * @return this instance, casted
     * @deprecated not needed anymore
     */
    @SuppressWarnings("unchecked")
    default ChannelInitializer unchecked() {
        return this;
    }
}
