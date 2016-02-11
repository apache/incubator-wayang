package org.qcri.rheem.java.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.java.plugin.JavaPlatform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility to handle the {@link ChannelInitializer}s of the {@link JavaPlatform}.
 */
public class Channels {

    private static final Map<Class<? extends Channel>, ChannelInitializer<?>> CHANNEL_INITIALIZERS;

    static {
        CHANNEL_INITIALIZERS = new HashMap<>();
        CHANNEL_INITIALIZERS.put(CollectionChannel.class, new CollectionChannel.Initializer());
        CHANNEL_INITIALIZERS.put(StreamChannel.class, new StreamChannel.Initializer());
        CHANNEL_INITIALIZERS.put(HdfsFile.class, new HdfsFileInitializer());
    }

    private static List<Class<? extends Channel>> supportedChannels = null;

    /**
     * @return a list of {@link Channel}s that are supported by the {@link JavaPlatform}
     */
    public static List<Class<? extends Channel>> getSupportedChannels() {
        if (supportedChannels == null) {
            supportedChannels = CHANNEL_INITIALIZERS.keySet().stream().collect(Collectors.toList());
        }
        return supportedChannels;
    }

    /**
     * Retrieve a requested {@link ChannelInitializer}.
     *
     * @see JavaPlatform#getChannelInitializer(Class)
     */
    @SuppressWarnings("unchecked")
    public static <T extends Channel> ChannelInitializer<T> getChannelInitializer(Class<T> channelClass) {
        return (ChannelInitializer<T>) CHANNEL_INITIALIZERS.get(channelClass);
    }


}
