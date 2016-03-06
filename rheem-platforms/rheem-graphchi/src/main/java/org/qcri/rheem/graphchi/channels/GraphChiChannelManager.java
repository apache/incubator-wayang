package org.qcri.rheem.graphchi.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.graphchi.GraphChiPlatform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link GraphChiChannelManager} for the {@link GraphChiPlatform}.
 */
public class GraphChiChannelManager extends DefaultChannelManager {

    public static final FileChannel.Descriptor HDFS_TSV_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "tsv");
    /**
     * {@link ChannelInitializer}s by class.
     */
    private final Map<ChannelDescriptor, ChannelInitializer> channelInitializers = new HashMap<>();

    public GraphChiChannelManager(GraphChiPlatform graphChiPlatform) {
        super(graphChiPlatform,
                HDFS_TSV_DESCRIPTOR,
                HDFS_TSV_DESCRIPTOR);

        this.channelInitializers.put(HDFS_TSV_DESCRIPTOR, new FileChannels.Initializer());
    }

    @Override
    public ChannelInitializer getChannelInitializer(ChannelDescriptor channelClass) {
        return this.channelInitializers.get(channelClass);
    }

    @Override
    public Map<ChannelDescriptor, Channel> setUpSourceSide(Junction junction, List<ChannelDescriptor> preferredChannelDescriptors) {
        // The GraphChi uses a simpler model for Channels: it waives internal Channels and operates directly on
        // external Channels.

        // Expect only a single type of ChannelDescriptor.
        final ChannelDescriptor channelDescriptor = preferredChannelDescriptors.stream()
                .reduce((cd1, cd2) -> {
                    assert cd1.equals(cd2);
                    return cd1;
                })
                .orElseThrow(() -> new IllegalStateException("Not a single ChannelDescriptor given."));

        // Create the Channel.
        final ChannelInitializer channelInitializer = this.getChannelInitializer(channelDescriptor);
        final Tuple<Channel, Channel> channelSetup = channelInitializer.setUpOutput(channelDescriptor, junction.getSourceOutput());
        junction.setSourceChannel(channelSetup.getField0());

        // Construct and return the result.
        Map<ChannelDescriptor, Channel> result = new HashMap<>(preferredChannelDescriptors.size());
        for (ChannelDescriptor preferredChannelDescriptor : preferredChannelDescriptors) {
            result.put(preferredChannelDescriptor, channelSetup.getField1());
        }
        return result;
    }

    @Override
    public void setUpTargetSide(Junction junction, int targetIndex, Channel externalChannel) {
        // The GraphChi uses a simpler model for Channels: it waives internal Channels and operates directly on
        // external Channels.

        junction.setTargetChannel(targetIndex, externalChannel);
    }
}
