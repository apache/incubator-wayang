package org.qcri.rheem.graphchi.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.graphchi.GraphChiPlatform;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ChannelManager} for the {@link GraphChiPlatform}.
 */
public class ChannelManager extends DefaultChannelManager {

    public static final FileChannel.Descriptor HDFS_TSV_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "tsv");
    /**
     * {@link ChannelInitializer}s by class.
     */
    private final Map<ChannelDescriptor, ChannelInitializer> channelInitializers = new HashMap<>();

    public ChannelManager(GraphChiPlatform graphChiPlatform) {
        super(graphChiPlatform,
                HDFS_TSV_DESCRIPTOR,
                HDFS_TSV_DESCRIPTOR);

        this.channelInitializers.put(HDFS_TSV_DESCRIPTOR, new HdfsFiles.Initializer());
    }

    @Override
    public ChannelInitializer getChannelInitializer(ChannelDescriptor channelClass) {
        return this.channelInitializers.get(channelClass);
    }

}
