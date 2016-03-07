package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.java.JavaPlatform;

/**
 * {@link ChannelInitializer} for the {@link JavaPlatform}.
 */
public interface JavaChannelInitializer extends ChannelInitializer {

    /**
     * Let the given {@code channel} provide its contents as a {@link StreamChannel}.
     *
     * @param optimizationContext provides estimates and accepts new {@link Operator}s
     */
    StreamChannel provideStreamChannel(Channel channel, OptimizationContext optimizationContext);

    /**
     * @return a {@link JavaChannelManager}
     */
    default JavaChannelManager getChannelManager() {
        return JavaPlatform.getInstance().getChannelManager();
    }

}
