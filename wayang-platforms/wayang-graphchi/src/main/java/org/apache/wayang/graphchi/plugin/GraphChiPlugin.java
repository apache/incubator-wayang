package org.apache.wayang.graphchi.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.fs.LocalFileSystem;
import org.apache.wayang.graphchi.channels.ChannelConversions;
import org.apache.wayang.graphchi.mappings.Mappings;
import org.apache.wayang.graphchi.platform.GraphChiPlatform;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} activates default capabilities of the {@link GraphChiPlatform}.
 */
public class GraphChiPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singleton(GraphChiPlatform.getInstance());
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
        final File localTempDir = LocalFileSystem.findTempDir();
        if (localTempDir != null) {
            configuration.setProperty("wayang.graphchi.tempdir", localTempDir.toString());
        }
    }

}
