package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.Mappings;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 * Activator for graph operations being executed with Rheem's basic operators.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class RheemBasicGraph implements Plugin {

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
