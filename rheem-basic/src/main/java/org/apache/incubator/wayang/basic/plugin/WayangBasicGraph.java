package org.apache.incubator.wayang.basic.plugin;

import org.apache.incubator.wayang.basic.mapping.Mappings;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.plugin.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 * Activator for graph operations being executed with Wayang's basic operators.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class WayangBasicGraph implements Plugin {

    @Override
    public void setProperties(Configuration configuration) {
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.GRAPH_MAPPINGS;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Collections.emptyList();
    }

}
