package org.apache.wayang.basic.plugin;

import org.apache.wayang.basic.mapping.Mappings;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;

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
