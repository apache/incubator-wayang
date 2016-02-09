package org.qcri.rheem.core.plan.executionplan;

import org.qcri.rheem.core.platform.Platform;

/**
 * Sets up the usage of {@link Channel} in an {@link ExecutionPlan} for a given {@link Platform}.
 */
public interface ChannelInitializer<T extends Channel> {

    /**
     * <i>Optional operation.</i> Implements the {@link Channel} on the {@code index}-th output of the given
     * {@link ExecutionTask}, thereby adding potentially further {@link ExecutionTask}s required for using the
     * {@link Channel}.
     *
     * @param executionTask that should output to the new {@link Channel}
     * @param index         the output index of the {@code executionTask} that should feed the {@link Channel}
     * @return the newly created and set up {@link Channel}
     */
    T setUpOutput(ExecutionTask executionTask, int index);

    /**
     * <i>Optional operation.</i> Implements the {@link Channel} on the {@code index}-th output of the given
     * {@link ExecutionTask}, thereby adding potentially further {@link ExecutionTask}s required for using the
     * {@link Channel}.
     *
     * @param channel       the {@link Channel} to be consumed as input
     * @param executionTask that should output to the new {@link Channel}
     * @param index         the output index of the {@code executionTask} that should feed the {@link Channel}
     * @return the newly created and set up {@link Channel}
     */
    void setUpInput(T channel, ExecutionTask executionTask, int index);

    /**
     * @return whether the {@link Channel}s managed by this instance are reusable
     */
    boolean isReusable();
}
