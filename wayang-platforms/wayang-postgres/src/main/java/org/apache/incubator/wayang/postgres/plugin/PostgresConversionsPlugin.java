package org.apache.incubator.wayang.postgres.plugin;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.plugin.Plugin;
import org.apache.incubator.wayang.java.platform.JavaPlatform;
import org.apache.incubator.wayang.postgres.channels.ChannelConversions;
import org.apache.incubator.wayang.postgres.platform.PostgresPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link PostgresPlatform}.
 */
public class PostgresConversionsPlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(PostgresPlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
    }
}
