package io.rheem.rheem.basic.plugin;

import io.rheem.rheem.basic.mapping.Mappings;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.plugin.Plugin;
import io.rheem.rheem.core.util.ReflectionUtils;
import io.rheem.rheem.core.util.fs.LocalFileSystem;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * Activator for the basic Rheem package.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class RheemBasic implements Plugin {

    public static final String TEMP_DIR_PROPERTY = "rheem.basic.tempdir";

    private static final String RHEEM_BASIC_DEFAULTS_PROPERTIES = "rheem-basic-defaults.properties";

    @Override
    public void setProperties(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(RHEEM_BASIC_DEFAULTS_PROPERTIES));
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
