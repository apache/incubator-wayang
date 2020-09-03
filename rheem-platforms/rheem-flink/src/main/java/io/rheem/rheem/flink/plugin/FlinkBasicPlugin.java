package io.rheem.rheem.flink.plugin;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.plugin.Plugin;
import io.rheem.rheem.flink.channels.ChannelConversions;
import io.rheem.rheem.flink.mapping.Mappings;
import io.rheem.rheem.flink.platform.FlinkPlatform;
import io.rheem.rheem.java.platform.JavaPlatform;

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
