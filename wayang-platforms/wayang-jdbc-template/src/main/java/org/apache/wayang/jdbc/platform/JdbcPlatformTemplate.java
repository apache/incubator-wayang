/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.DatabaseDescriptor;
import org.apache.wayang.jdbc.execution.JdbcExecutor;

import java.sql.Connection;

/**
 * {@link Platform} implementation for a JDBC-accessible database.
 */
public abstract class JdbcPlatformTemplate extends Platform {

    public final String cpuMhzProperty = String.format("wayang.%s.cpu.mhz", this.getPlatformId());

    public final String coresProperty = String.format("wayang.%s.cores", this.getPlatformId());

    public final String jdbcUrlProperty = String.format("wayang.%s.jdbc.url", this.getPlatformId());

    public final String jdbcUserProperty = String.format("wayang.%s.jdbc.user", this.getPlatformId());

    public final String jdbcPasswordProperty = String.format("wayang.%s.jdbc.password", this.getPlatformId());

    private String getDefaultConfigurationFile() {
        return String.format("wayang-%s-defaults.properties", this.getPlatformId());
    }

    /**
     * {@link ChannelDescriptor} for {@link SqlQueryChannel}s with this instance.
     */
    private final SqlQueryChannel.Descriptor sqlQueryChannelDescriptor = new SqlQueryChannel.Descriptor(this);

    public Connection getConnection() {
        return connection;
    }

    private Connection connection = null;

    protected JdbcPlatformTemplate(String platformName, String configName) {
        super(platformName, configName);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(this.getDefaultConfigurationFile()));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JdbcExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(this.cpuMhzProperty);
        int numCores = (int) configuration.getLongProperty(this.coresProperty);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000.)),
                LoadToTimeConverter.createLinearCoverter(0),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty(String.format("wayang.%s.costs.fix", this.getPlatformId())),
                configuration.getDoubleProperty(String.format("wayang.%s.costs.per-ms", this.getPlatformId()))
        );
    }

    /**
     * Provide a unique identifier for this kind of platform. Should consist of alphanumerical characters only.
     *
     * @return the platform ID
     */
    public String getPlatformId() {
        return this.getConfigurationName();
    }

    /**
     * Provide the name of the JDBC driver {@link Class} for this instance.
     *
     * @return the driver {@link Class} name
     */
    protected abstract String getJdbcDriverClassName();

    /**
     * Retrieve a {@link SqlQueryChannel.Descriptor} for this instance.
     *
     * @return the {@link SqlQueryChannel.Descriptor}
     */
    public SqlQueryChannel.Descriptor getSqlQueryChannelDescriptor() {
        return this.sqlQueryChannelDescriptor;
    }

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
