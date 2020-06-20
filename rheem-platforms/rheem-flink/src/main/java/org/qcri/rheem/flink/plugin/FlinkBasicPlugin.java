package org.qcri.rheem.flink.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.flink.channels.ChannelConversions;
import org.qcri.rheem.flink.mapping.Mappings;
import org.qcri.rheem.flink.platform.FlinkPlatform;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Plugin} enables to use the basic Rheem {@link Operator}s on the {@link FlinkPlatform}.
 */
public class FlinkBasicPlugin implements Plugin{
    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(FlinkPlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.BASIC_MAPPINGS;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {

    }
}
