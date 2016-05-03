package org.qcri.rheem.basic.plugin;

import org.qcri.rheem.basic.mapping.GlobalReduceMapping;
import org.qcri.rheem.basic.mapping.MaterializedGroupByMapping;
import org.qcri.rheem.basic.mapping.ReduceByMapping;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Activator for the basic Rheem package.
 */
@SuppressWarnings("unused") // It's loaded via reflection.
public class RheemBasicPlatform extends Platform {

    public static final String TEMP_DIR_PROPERTY = "rheem.basic.tempdir";

    private static final String RHEEM_BASIC_DEFAULTS_PROPERTIES = "/rheem-basic-defaults.properties";

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static RheemBasicPlatform instance = null;

    public static RheemBasicPlatform getInstance() {
        if (instance == null) {
            instance = new RheemBasicPlatform();
        }
        return instance;
    }

    public RheemBasicPlatform() {
        super("Rheem Basic");
        this.initConfiguration();
        this.initMappings();
    }

    private void initConfiguration() {
        final Configuration defaultConfiguration = Configuration.getDefaultConfiguration();
        defaultConfiguration.load(RheemBasicPlatform.class.getResourceAsStream(RHEEM_BASIC_DEFAULTS_PROPERTIES));
        final String localTempDir = findLocalTempDir();
        if (localTempDir != null) {
            defaultConfiguration.setProperty(TEMP_DIR_PROPERTY, localTempDir);
        }
    }

    private static String findLocalTempDir() {
        try {
            final File tempFile = File.createTempFile("rheem", "probe");
            tempFile.deleteOnExit();
            return tempFile.getParentFile().toURI().toURL().toString();
        } catch (IOException e) {
            LoggerFactory.getLogger(RheemBasicPlatform.class).warn("Could not determine local temp directory.", e);
            return null;
        }
    }

    private void initMappings() {
        this.mappings.add(new ReduceByMapping());
        this.mappings.add(new MaterializedGroupByMapping());
        this.mappings.add(new GlobalReduceMapping());
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        throw new RheemException("Platform is not executable.");
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }

    @Override
    public void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph) {
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        throw new RuntimeException("This platform has no ExecutionOperators.");
    }
}
