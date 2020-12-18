package io.rheem.rheem.core.plan.executionplan;

import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.OutputSlot;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.util.Tuple;

/**
 * Sets up the usage of {@link Channel} in an {@link ExecutionPlan} for a given {@link Platform}.
 */
public interface ChannelInitializer {

    /**
     * <i>Optional operation.</i>
     * Creates a new {@link Channel} adjacent* to an {@link ExecutionOperator}'s {@code outputSlot}.
     * <p>* Note that in general the created {@link Channel} is not necessarily directly adjacent to the {@code outputSlot},
     * but a chain {@link Channel}s (and {@link ExecutionTask}s) might be in betweeen.</p>
     *
     * @param descriptor          describes the {@link Channel} to be created
     * @param outputSlot          whose output the {@link Channel} should accept
     * @param optimizationContext provides estimates and accepts new {@link Operator}s
     * @return {@link Channel} that is directly output by the {@code outputSlot} and the {@link Channel} that was
     * actually requested; both are interlinked.
     */
    Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext);

    /**
     * <i>Optional operation.</i>
     * Creates a new {@link Channel} incident* to the {@code source}.
     * <p>* Note that in general the created {@link Channel} is not necessarily directly incident to the {@code source},
     * but a chain {@link Channel}s (and {@link ExecutionTask}s) might be in betweeen.</p>
     *
     * @param descriptor          describes the {@link Channel} to be created
     * @param source              that should be exposed as a new {@link Channel}
     * @param optimizationContext provides estimates and accepts new {@link Operator}s
     * @return the set up {@link Channel}
     */
    Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext);

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
