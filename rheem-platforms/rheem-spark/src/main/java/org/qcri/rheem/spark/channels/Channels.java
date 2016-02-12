package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
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

    private static final Map<Class<? extends Channel>, ChannelTypeDescriptor<?>> CHANNEL_TYPE_DESCRIPTORS;

    static {
        CHANNEL_TYPE_DESCRIPTORS = new HashMap<>();
        CHANNEL_TYPE_DESCRIPTORS.put(RddChannel.class, new ChannelTypeDescriptor<>(
                new RddChannel.Initializer(),
                channel -> new RddChannel.Executor(channel.getConsumers().size() > 2)
        ));
        CHANNEL_TYPE_DESCRIPTORS.put(HdfsFile.class, new ChannelTypeDescriptor<>(
                new HdfsFileInitializer(),
                channel -> new HdfsFileInitializer.Executor((HdfsFile) channel))
        );
    }

    private static List<Class<? extends Channel>> supportedChannels = null;

    /**
     * @return a list of {@link Channel}s that are supported by the {@link SparkPlatform}
     */
    public static List<Class<? extends Channel>> getSupportedChannels() {
        if (supportedChannels == null) {
            supportedChannels = CHANNEL_TYPE_DESCRIPTORS.keySet().stream().collect(Collectors.toList());
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
        final ChannelTypeDescriptor<?> channelTypeDescriptor = CHANNEL_TYPE_DESCRIPTORS.get(channelClass);
        return channelTypeDescriptor == null ? null : (ChannelInitializer<T>) channelTypeDescriptor.getInitializer();
    }

    /**
     * Retrieve a requested {@link ChannelInitializer}.
     *
     * @see SparkPlatform#getChannelInitializer(Class)
     */
    @SuppressWarnings("unchecked")
    public static ChannelExecutor createChannelExecutor(Channel channel) {
        final ChannelTypeDescriptor<?> channelTypeDescriptor = CHANNEL_TYPE_DESCRIPTORS.get(channel.getClass());
        return channelTypeDescriptor == null ? null : channelTypeDescriptor.getExecutorFactory().apply(channel);
    }


}
