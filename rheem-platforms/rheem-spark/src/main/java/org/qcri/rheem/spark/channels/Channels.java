package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility to handle the {@link ChannelInitializer}s of the {@link SparkPlatform}.
 */
public class Channels {

    private static final Map<Class<? extends Channel>, ChannelInitializer<?>> CHANNEL_INITIALIZERS;

    static {
        CHANNEL_INITIALIZERS = new HashMap<>();
        CHANNEL_INITIALIZERS.put(RddChannel.class, new RddChannel.Initializer());
        CHANNEL_INITIALIZERS.put(HdfsFile.class, new HdfsFileInitializer());
    }

    private static List<Class<? extends Channel>> supportedChannels = null;

    /**
     * @return a list of {@link Channel}s that are supported by the {@link SparkPlatform}
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
     * @see SparkPlatform#getChannelInitializer(Class)
     */
    @SuppressWarnings("unchecked")
    public static <T extends Channel> ChannelInitializer<T> getChannelInitializer(Class<T> channelClass) {
        final ChannelInitializer<?> channelInitializer = CHANNEL_INITIALIZERS.get(channelClass);
        if (channelInitializer == null) {
            throw new RheemException(String.format("%s does not work with %s.",
                    SparkPlatform.class.getSimpleName(), channelClass.getSimpleName()));
        }
        return (ChannelInitializer<T>) channelInitializer;
    }


}
