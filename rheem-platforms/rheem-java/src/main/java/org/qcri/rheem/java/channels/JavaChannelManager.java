package org.qcri.rheem.java.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Tuple;
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

    private final Map<ChannelDescriptor, ChannelTypeDescriptor> channelTypeDescriptors = new LinkedHashMap<>();

    private final List<ChannelDescriptor> supportedChannels = new LinkedList<>();

    private final List<ChannelDescriptor> supportedBroadcastChannels = new LinkedList<>();

    public JavaChannelManager(Platform platform) {
        super(platform, CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
        this.initializeChannelTypeDescriptors();
    }

    private void initializeChannelTypeDescriptors() {
        this.addChannel(StreamChannel.DESCRIPTOR,
                new StreamChannel.Initializer(),
                channel -> new StreamChannel.Executor(channel.isMarkedForInstrumentation()),
                true, false);
        this.addChannel(CollectionChannel.DESCRIPTOR,
                new CollectionChannel.Initializer(),
                channel -> new CollectionChannel.Executor(channel.isMarkedForInstrumentation()),
                true, true);
        this.addChannel(new FileChannel.Descriptor("hdfs", "object-file"),
                new HdfsFileInitializer(),
                channel -> new HdfsFileInitializer.Executor((FileChannel) channel),
                true, true);
        this.addChannel(new FileChannel.Descriptor("hdfs", "tsv"),
                new HdfsFileInitializer(),
                channel -> new HdfsFileInitializer.Executor((FileChannel) channel),
                true, true);
    }

    private void addChannel(ChannelDescriptor channelClass,
                            JavaChannelInitializer channelInitializer,
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

    public List<ChannelDescriptor> getSupportedChannels() {
        return this.supportedChannels;
    }

    public List<ChannelDescriptor> getSupportedBroadcastChannels() {
        return this.supportedBroadcastChannels;
    }

    @Override
    public JavaChannelInitializer getChannelInitializer(ChannelDescriptor channelClass) {
        return (JavaChannelInitializer) this.channelTypeDescriptors.get(channelClass).getInitializer();
    }

    public ChannelExecutor createChannelExecutor(Channel channel) {
        return this.channelTypeDescriptors.get(channel.getDescriptor()).getExecutorFactory().apply(channel);
    }

    @Override
    public boolean exchangeWithInterstageCapable(Channel channel) {
        if (channel instanceof StreamChannel) {
            ((StreamChannel) channel).exchangeWith(CollectionChannel.DESCRIPTOR);
            return true;
        }
        return super.exchangeWithInterstageCapable(channel);
    }
}
