package org.qcri.rheem.java.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.java.JavaPlatform;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * {@link ChannelManager} of the {@link JavaPlatform}.
 */
public class JavaChannelManager extends DefaultChannelManager {

    private final Map<Class<? extends Channel>, ChannelTypeDescriptor> channelTypeDescriptors = new LinkedHashMap<>();

    private final List<Class<? extends Channel>> supportedChannels = new LinkedList<>();

    private final List<Class<? extends Channel>> supportedBroadcastChannels = new LinkedList<>();

    public JavaChannelManager(Platform platform) {
        super(platform, CollectionChannel.class, StreamChannel.class);
        this.initializeChannelTypeDescriptors();
    }

    private void initializeChannelTypeDescriptors() {
        this.addChannel(StreamChannel.class,
                new StreamChannel.Initializer(),
                channel -> new StreamChannel.Executor(),
                true, false);
        this.addChannel(CollectionChannel.class,
                new CollectionChannel.Initializer(),
                channel -> new CollectionChannel.Executor(),
                true, true);
        this.addChannel(HdfsFile.class,
                new HdfsFileInitializer(),
                channel -> new HdfsFileInitializer.Executor((HdfsFile) channel),
                true, true);
    }

    private void addChannel(Class<? extends Channel> channelClass,
                            ChannelInitializer channelInitializer,
                            Function<Channel, ChannelExecutor> executorFactory,
                            boolean isRegularChannel,
                            boolean isBroadcastChannel) {
        final ChannelTypeDescriptor channelTypeDescriptor = new ChannelTypeDescriptor(channelInitializer, executorFactory);
        this.channelTypeDescriptors.put(channelClass, channelTypeDescriptor);
        if (isRegularChannel) {
            this.supportedChannels.add(channelClass);
        }
        if (isBroadcastChannel) {
            this.supportedBroadcastChannels.add(channelClass);
        }
    }

    public List<Class<? extends Channel>> getSupportedChannels() {
        return this.supportedChannels;
    }

    public List<Class<? extends Channel>> getSupportedBroadcastChannels() {
        return this.supportedBroadcastChannels;
    }

    @Override
    public ChannelInitializer getChannelInitializer(Class<? extends Channel> channelClass) {
        return this.channelTypeDescriptors.get(channelClass).getInitializer();
    }

    public ChannelExecutor createChannelExecutor(Channel channel) {
        return this.channelTypeDescriptors.get(channel.getClass()).getExecutorFactory().apply(channel);
    }
}
