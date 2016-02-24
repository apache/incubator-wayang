package org.qcri.rheem.graphchi.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.DefaultChannelManager;
import org.qcri.rheem.graphchi.GraphChiPlatform;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ChannelManager} for the {@link GraphChiPlatform}.
 */
public class ChannelManager extends DefaultChannelManager {

    /**
     * {@link ChannelInitializer}s by class.
     */
    private final Map<Class<? extends Channel>, ChannelInitializer> channelInitializers = new HashMap<>();

    public ChannelManager(GraphChiPlatform graphChiPlatform) {
        super(graphChiPlatform,
                HdfsFile.class,
                HdfsFile.class);

        this.channelInitializers.put(HdfsFile.class, new HdfsFiles.Initializer());
    }

    @Override
    public ChannelInitializer getChannelInitializer(Class<? extends Channel> channelClass) {
        return this.channelInitializers.get(channelClass);
    }

}
