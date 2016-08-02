package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.GlobalReduceMapping;
import org.qcri.rheem.basic.mapping.MaterializedGroupByMapping;
import org.qcri.rheem.basic.mapping.ReduceByMapping;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
        final String localTempDir = findLocalTempDir();
        if (localTempDir != null) {
            configuration.setProperty(TEMP_DIR_PROPERTY, localTempDir);
        }
    }

    private static String findLocalTempDir() {
        try {
            final File tempFile = File.createTempFile("rheem", "probe");
            tempFile.deleteOnExit();
            return tempFile.getParentFile().toURI().toURL().toString();
        } catch (IOException e) {
            LoggerFactory.getLogger(RheemBasic.class).warn("Could not determine local temp directory.", e);
            return null;
        }
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Arrays.asList(new ReduceByMapping(), new MaterializedGroupByMapping(), new GlobalReduceMapping());
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
