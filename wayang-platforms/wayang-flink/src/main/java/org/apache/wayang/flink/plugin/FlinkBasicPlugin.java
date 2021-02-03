package org.apache.wayang.flink.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.flink.channels.ChannelConversions;
import org.apache.wayang.flink.mapping.Mappings;
import org.apache.wayang.flink.platform.FlinkPlatform;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Plugin} enables to use the basic Wayang {@link Operator}s on the {@link FlinkPlatform}.
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
