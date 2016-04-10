package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yidris on 3/23/16.
 */
public class PostgresChannelManager extends DefaultChannelManager {

    public static final FileChannel.Descriptor HDFS_TSV_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "tsv");
    public static final ChannelDescriptor INTERNAL_DESCRIPTOR = PostgresInternalChannel.DESCRIPTOR;

    /**
     * {@link ChannelInitializer}s by class.
     */
    private final Map<ChannelDescriptor, ChannelInitializer> channelInitializers = new HashMap<>();

    public PostgresChannelManager(Platform platform) {
        super(platform,
                HDFS_TSV_DESCRIPTOR,
                HDFS_TSV_DESCRIPTOR);

        this.channelInitializers.put(HDFS_TSV_DESCRIPTOR, new FileChannelInitializer());
        //this.channelInitializers.put(INTERNAL_DESCRIPTOR, new PostgresInternalChannel.Initializer());
    }

    @Override
    public ChannelInitializer getChannelInitializer(ChannelDescriptor channelDescriptor) {
        return this.channelInitializers.get(channelDescriptor);
    }

    @Override
    public Map<ChannelDescriptor, Channel> setUpSourceSide(Junction junction, List<ChannelDescriptor> preferredChannelDescriptors, OptimizationContext optimizationContext) {

        // Expect only a single type of ChannelDescriptor.
        final ChannelDescriptor channelDescriptor = preferredChannelDescriptors.stream()
                .reduce((cd1, cd2) -> {
                    assert cd1.equals(cd2);
                    return cd1;
                })
                .orElseThrow(() -> new IllegalStateException("Not a single ChannelDescriptor given."));

        // Create the Channel.
        final ChannelInitializer channelInitializer = this.getChannelInitializer(channelDescriptor);
        final Tuple<Channel, Channel> channelSetup = channelInitializer.setUpOutput(channelDescriptor, junction.getSourceOutput(), optimizationContext);
        junction.setSourceChannel(channelSetup.getField0());

        // Construct and return the result.
        Map<ChannelDescriptor, Channel> result = new HashMap<>(preferredChannelDescriptors.size());
        for (ChannelDescriptor preferredChannelDescriptor : preferredChannelDescriptors) {
            result.put(preferredChannelDescriptor, channelSetup.getField1());
        }
        return result;
    }

    @Override
    public void setUpTargetSide(Junction junction, int targetIndex, Channel externalChannel,
                                OptimizationContext optimizationContext) {


        junction.setTargetChannel(targetIndex, externalChannel);
    }
}
