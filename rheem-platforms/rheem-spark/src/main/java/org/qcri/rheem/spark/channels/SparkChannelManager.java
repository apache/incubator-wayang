package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelDescriptor;
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

    private final Map<ChannelDescriptor, ChannelTypeDescriptor> channelTypeDescriptors = new LinkedHashMap<>();

    private final List<ChannelDescriptor> supportedChannels = new LinkedList<>();

    private final List<ChannelDescriptor> supportedBroadcastChannels = new LinkedList<>();

    private final List<ChannelDescriptor> allSupportedChannels = new LinkedList<>();

    public SparkChannelManager(Platform platform) {
        super(platform, RddChannel.DESCRIPTOR, BroadcastChannel.DESCRIPTOR);
        this.initializeChannelTypeDescriptors();
    }

    private void initializeChannelTypeDescriptors() {
        this.addChannel(RddChannel.DESCRIPTOR,
                new RddChannel.Initializer(),
                (channel, sparkExecutor) ->
                        new ChannelExecutor.ForRDD(channel, channel.getConsumers().size() > 1, sparkExecutor),
                true, false);

        this.addChannel(BroadcastChannel.DESCRIPTOR,
                new BroadcastChannel.Initializer(),
                (channel, sparkExecutor) -> new ChannelExecutor.ForBroadcast(channel),
                false, true);

        this.addChannel(new FileChannel.Descriptor("hdfs", "object-file"),
                new HdfsFileInitializer(),
                (channel, sparkExecutor) -> new HdfsFileInitializer.Executor((FileChannel) channel),
                true, false);

        this.addChannel(new FileChannel.Descriptor("hdfs", "tsv"),
                new HdfsFileInitializer(),
                (channel, sparkExecutor) -> new HdfsFileInitializer.Executor((FileChannel) channel),
                true, false);
    }

    private void addChannel(ChannelDescriptor channelClass,
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

    public List<ChannelDescriptor> getSupportedChannels() {
        return this.supportedChannels;
    }

    public List<ChannelDescriptor> getSupportedBroadcastChannels() {
        return this.supportedBroadcastChannels;
    }

    @Override
    public ChannelInitializer getChannelInitializer(ChannelDescriptor channelClass) {
        return this.channelTypeDescriptors.get(channelClass).getInitializer();
    }

    public List<ChannelDescriptor> getAllSupportedChannels() {
        return this.allSupportedChannels;
    }

    public ChannelExecutor createChannelExecutor(Channel channel, SparkExecutor sparkExecutor) {
        return this.channelTypeDescriptors.get(channel.getDescriptor()).getExecutorFactory().apply(channel, sparkExecutor);
    }
}
