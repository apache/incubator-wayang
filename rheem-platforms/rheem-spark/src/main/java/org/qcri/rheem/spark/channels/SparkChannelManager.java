package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * {@link ChannelManager} of the {@link SparkPlatform}.
 */
public class SparkChannelManager extends DefaultChannelManager {

    private final Map<Class<? extends Channel>, ChannelTypeDescriptor> channelTypeDescriptors = new LinkedHashMap<>();

    private final List<Class<? extends Channel>> supportedChannels = new LinkedList<>();

    private final List<Class<? extends Channel>> supportedBroadcastChannels = new LinkedList<>();

    private final List<Class<? extends Channel>> allSupportedChannels = new LinkedList<>();

    public SparkChannelManager(Platform platform) {
        super(platform, RddChannel.class, BroadcastChannel.class);
        this.initializeChannelTypeDescriptors();
    }

    private void initializeChannelTypeDescriptors() {
        this.addChannel(RddChannel.class,
                new RddChannel.Initializer(),
                (channel, sparkExecutor) ->
                        new ChannelExecutor.ForRDD(channel, channel.getConsumers().size() > 1, sparkExecutor),
                true, false);

        this.addChannel(BroadcastChannel.class,
                new BroadcastChannel.Initializer(),
                (channel, sparkExecutor) -> new ChannelExecutor.ForBroadcast(channel),
                false, true);

        this.addChannel(HdfsFile.class,
                new HdfsFileInitializer(),
                (channel, sparkExecutor) -> new HdfsFileInitializer.Executor((HdfsFile) channel),
                true, false);
    }

    private void addChannel(Class<? extends Channel> channelClass,
                            ChannelInitializer channelInitializer,
                            BiFunction<Channel, SparkExecutor, ChannelExecutor> executorFactory,
                            boolean isRegularChannel,
                            boolean isBroadcastChannel) {
        final ChannelTypeDescriptor channelTypeDescriptor = new ChannelTypeDescriptor(channelInitializer, executorFactory);
        this.channelTypeDescriptors.put(channelClass, channelTypeDescriptor);
        this.allSupportedChannels.add(channelClass);
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

    public List<Class<? extends Channel>> getAllSupportedChannels() {
        return this.allSupportedChannels;
    }

    public ChannelExecutor createChannelExecutor(Channel channel, SparkExecutor sparkExecutor) {
        return this.channelTypeDescriptors.get(channel.getClass()).getExecutorFactory().apply(channel, sparkExecutor);
    }
}
