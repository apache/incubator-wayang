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

package org.apache.wayang.giraph.platform;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.giraph.execution.GiraphExecutor;

/**
 * Giraph {@link Platform} for Wayang.
 */
public class GiraphPlatform extends Platform {
    public static final String CPU_MHZ_PROPERTY = "wayang.giraph.cpu.mhz";

    public static final String CORES_PROPERTY = "wayang.giraph.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "wayang.giraph.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "wayang-giraph-defaults.properties";

    private static GiraphPlatform instance;


    protected GiraphPlatform() {
        super("Giraph", "giraph");
        this.initialize();
    }

    /**
     * Initializes this instance.
     */
    private void initialize() {

    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    public static GiraphPlatform getInstance() {
        if (instance == null) {
            instance = new GiraphPlatform();
        }
        return instance;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new GiraphExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(CPU_MHZ_PROPERTY);
        int numCores = (int) configuration.getLongProperty(CORES_PROPERTY);
        double hdfsMsPerMb = configuration.getDoubleProperty(HDFS_MS_PER_MB_PROPERTY);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.giraph.costs.fix"),
                configuration.getDoubleProperty("wayang.giraph.costs.per-ms")
        );
    }
}
