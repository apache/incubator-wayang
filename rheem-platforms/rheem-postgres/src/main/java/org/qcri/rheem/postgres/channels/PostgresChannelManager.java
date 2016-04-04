package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.core.platform.Platform;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yidris on 3/23/16.
 */
public class PostgresChannelManager extends DefaultChannelManager {

    public static final FileChannel.Descriptor HDFS_TSV_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "tsv");

    /**
     * {@link ChannelInitializer}s by class.
     */
    private final Map<ChannelDescriptor, ChannelInitializer> channelInitializers = new HashMap<>();

    public PostgresChannelManager(Platform platform) {
        super(platform,
                HDFS_TSV_DESCRIPTOR,
                HDFS_TSV_DESCRIPTOR);

        this.channelInitializers.put(HDFS_TSV_DESCRIPTOR, new FileChannelInitializer());
    }

    @Override
    public ChannelInitializer getChannelInitializer(ChannelDescriptor channelDescriptor) {
        return this.channelInitializers.get(channelDescriptor);
    }
}
