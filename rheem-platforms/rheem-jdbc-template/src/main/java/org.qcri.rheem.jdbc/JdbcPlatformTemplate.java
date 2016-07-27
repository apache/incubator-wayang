package org.qcri.rheem.jdbc;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.jdbc.execution.DatabaseDescriptor;
import org.qcri.rheem.jdbc.execution.JdbcExecutor;

import java.sql.Connection;
import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link Platform} implementation for the PostgreSQL database.
 */
public abstract class JdbcPlatformTemplate extends Platform {

    public final String cpuMhzProperty = String.format("rheem.%s.cpu.mhz", this.getPlatformId());

    public final String coresProperty = String.format("rheem.%s.cores", this.getPlatformId());

    public final String hdfsMsPerMbProperty = String.format("rheem.%s.hdfs.ms-per-mb", this.getPlatformId());

    public final String jdbcUrlProperty = String.format("rheem.%s.jdbc.url", this.getPlatformId());

    public final String jdbcUserProperty = String.format("rheem.%s.jdbc.user", this.getPlatformId());

    public final String jdbcPasswordProperty = String.format("rheem.%s.jdbc.password", this.getPlatformId());

    public final String defaultConfigFile = String.format("rheem-%s-defaults.properties", this.getPlatformId());

    private DatabaseDescriptor databaseDescriptor;

    protected final Collection<Mapping> mappings = new LinkedList<>();

    private static JdbcPlatformTemplate instance = null;

    public Connection getConnection() {
        return connection;
    }

    private Connection connection = null;

    protected JdbcPlatformTemplate(String platformName) {
        super(platformName);
        this.initializeMappings();
        this.initializeConfiguration();
    }

    private void initializeConfiguration() {
        final Configuration defaultConfiguration = Configuration.getDefaultConfiguration();
        defaultConfiguration.load(ReflectionUtils.loadResource(this.defaultConfigFile));
    }

    protected abstract void initializeMappings();

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    public void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph) {

    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JdbcExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(this.cpuMhzProperty);
        int numCores = (int) configuration.getLongProperty(this.coresProperty);
        double hdfsMsPerMb = configuration.getDoubleProperty(this.hdfsMsPerMbProperty);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000.)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    /**
     * Provide a unique identifier for this kind of platform. Should consist of alphanumerical characters only.
     *
     * @return the platform ID
     */
    protected abstract String getPlatformId();

    /**
     * Provide the name of the JDBC driver {@link Class} for this instance.
     *
     * @return the driver {@link Class} name
     */
    protected abstract String getJdbcDriverClassName();

    /**
     * Creates a new {@link DatabaseDescriptor} for this instance and the given {@link Configuration}.
     *
     * @param configuration provides configuration information for the result
     * @return the {@link DatabaseDescriptor}
     */
    public DatabaseDescriptor createDatabaseDescriptor(Configuration configuration) {
        return new DatabaseDescriptor(
                configuration.getStringProperty(this.jdbcUrlProperty),
                configuration.getStringProperty(this.jdbcUserProperty, null),
                configuration.getStringProperty(this.jdbcPasswordProperty, null),
                this.getJdbcDriverClassName()
        );
    }
}