package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.java.JavaPlatform;

/**
 * {@link ChannelInitializer} for the {@link JavaPlatform}.
 */
public interface JavaChannelInitializer extends ChannelInitializer {

    /**
     * Let the given {@code channel} provide its contents as a {@link StreamChannel}.
     */
    StreamChannel provideStreamChannel(Channel channel);

    /**
     * @return a {@link JavaChannelManager}
     */
    default JavaChannelManager getChannelManager() {
        return JavaPlatform.getInstance().getChannelManager();
    }

}
