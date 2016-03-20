package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link ChannelInitializer} for the {@link SparkPlatform}.
 */
public interface SparkChannelInitializer extends ChannelInitializer {

    /**
     * Let the given {@code channel} provide its contents as a {@link RddChannel}. This instance should be the
     * responsible for the {@code channel}.
     */
    RddChannel provideRddChannel(Channel channel, OptimizationContext optimizationContext);

    /**
     * @return a {@link SparkChannelManager}
     */
    default SparkChannelManager getChannelManager() {
        return SparkPlatform.getInstance().getChannelManager();
    }

    /**
     * Derives an {@link RddChannel} from the {@code sourceChannel}.
     */
    default RddChannel createRddChannel(Channel sourceChannel, OptimizationContext optimizationContext) {
        final SparkChannelManager channelManager = this.getChannelManager();
        final SparkChannelInitializer sourceChannelInitializer = channelManager.getChannelInitializer(sourceChannel.getDescriptor());
        return  sourceChannelInitializer.provideRddChannel(sourceChannel, optimizationContext);
    }

}
