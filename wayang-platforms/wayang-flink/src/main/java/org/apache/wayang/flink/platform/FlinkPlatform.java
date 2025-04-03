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

package org.apache.wayang.flink.platform;

import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.wayang.basic.plugin.WayangBasic;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.flink.execution.FlinkContextReference;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * {@link Platform} for Apache Flink.
 */
public class FlinkPlatform extends Platform {
    private static final String PLATFORM_NAME = "Apache Flink";

    private static final String CONFIG_NAME = "flink";

    private static final String DEFAULT_CONFIG_FILE = "wayang-flink-defaults.properties";

    public static final String INITIALIZATION_MS_CONFIG_KEY = "wayang.flink.init.ms";

    private static FlinkPlatform instance = null;

    private static final String[] REQUIRED_FLINK_PROPERTIES = {
    };

    private static final String[] OPTIONAL_FLINK_PROPERTIES = {
    };

    /**
     * <i>Lazy-initialized.</i> Maintains a reference to a {@link ExecutionEnvironment}. This instance's reference, however,
     * does not hold a counted reference, so it might be disposed.
     */
    private FlinkContextReference flinkContextReference = null;

    private Logger logger = LogManager.getLogger(this.getClass());

    public static FlinkPlatform getInstance() {
        if (instance == null) {
            instance = new FlinkPlatform();
        }
        return instance;
    }

    private FlinkPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    /**
     * Configures the single maintained {@link ExecutionEnvironment} according to the {@code job} and returns it.
     *
     * @return a {@link FlinkContextReference} wrapping the {@link ExecutionEnvironment}
     */
    public FlinkContextReference getFlinkContext(Job job) {
        Configuration conf = job.getConfiguration();
        String[] jars = getJars(job);

        if(this.flinkContextReference == null)
            switch (conf.getStringProperty("wayang.flink.mode.run")) {
            case "local":
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.getExecutionEnvironment(),
                        (int) conf.getLongProperty("wayang.flink.parallelism")
                );
                break;
            case "distribution":
                org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
                flinkConfig.setString("rest.client.max-content-length", "1000000000");
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.createRemoteEnvironment(
                                conf.getStringProperty("wayang.flink.master"),
                                Integer.parseInt(conf.getStringProperty("wayang.flink.port")),
                                flinkConfig,
                                jars
                        ),
                        (int)conf.getLongProperty("wayang.flink.parallelism")
                );
                break;
            case "collection":
            default:
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        new CollectionEnvironment(),
                        1
                );
                break;
        }
        return this.flinkContextReference;

    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new FlinkExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("wayang.flink.cpu.mhz");
        int numCores = (int) ( configuration.getLongProperty("wayang.flink.parallelism"));
        double hdfsMsPerMb = configuration.getDoubleProperty("wayang.flink.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("wayang.flink.network.ms-per-mb");
        double stretch = configuration.getDoubleProperty("wayang.flink.stretch");
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(networkMsPerMb / 1000000d),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.flink.costs.fix"),
                configuration.getDoubleProperty("wayang.flink.costs.per-ms")
        );
    }


    private String[] getJars(Job job){
        List<String> jars = new ArrayList<>();
        List<Class> clazzs = Arrays.asList(new Class[]{FlinkPlatform.class, WayangBasic.class, WayangContext.class});

        clazzs.stream().map(
                ReflectionUtils::getDeclaringJar
        ).filter(
                element -> element != null
        ).forEach(jars::add);


        final Set<String> udfJarPaths = job.getUdfJarPaths();
        if (udfJarPaths.isEmpty()) {
            this.logger.warn("Non-local FlinkContext but not UDF JARs have been declared.");
        } else {
            udfJarPaths.stream().filter(a -> a != null).forEach(jars::add);
        }

        return jars.toArray(new String[0]);
    }
}
