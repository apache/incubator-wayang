package org.apache.wayang.basic.plugin;

import org.apache.wayang.basic.mapping.Mappings;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.fs.LocalFileSystem;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * Activator for the basic Wayang package.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class WayangBasic implements Plugin {

    public static final String TEMP_DIR_PROPERTY = "wayang.basic.tempdir";

    private static final String WAYANG_BASIC_DEFAULTS_PROPERTIES = "wayang-basic-defaults.properties";

    @Override
    public void setProperties(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(WAYANG_BASIC_DEFAULTS_PROPERTIES));
        final File localTempDir = LocalFileSystem.findTempDir();
        if (localTempDir != null) {
            configuration.setProperty(TEMP_DIR_PROPERTY, LocalFileSystem.toURL(localTempDir));
        }
    }


    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.BASIC_MAPPINGS;
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
