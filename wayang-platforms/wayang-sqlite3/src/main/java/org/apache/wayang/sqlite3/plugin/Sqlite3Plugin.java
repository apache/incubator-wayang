package org.apache.wayang.sqlite3.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.sqlite3.channels.ChannelConversions;
import org.apache.wayang.sqlite3.mapping.Mappings;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.Arrays;
import java.util.Collection;

/**
 * This {@link Plugin} enables to use some basic Wayang {@link Operator}s on the {@link Sqlite3Platform}.
 */
public class Sqlite3Plugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(Sqlite3Platform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
    }
}
