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

package org.apache.wayang.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.wayang.basic.plugin.WayangBasic;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.spark.execution.SparkContextReference;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkCollectionSource;
import org.apache.wayang.spark.operators.SparkLocalCallbackSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Set;

/**
 * {@link Platform} for Apache Spark.
 */
public class SparkPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Spark";

    private static final String CONFIG_NAME = "spark";

    private static final String DEFAULT_CONFIG_FILE = "wayang-spark-defaults.properties";

    public static final String INITIALIZATION_MS_CONFIG_KEY = "wayang.spark.init.ms";

    private static SparkPlatform instance = null;

    private static final String[] REQUIRED_SPARK_PROPERTIES = {
            "spark.master"
    };

    private static final String[] OPTIONAL_SPARK_PROPERTIES = {
            "spark.app.name",
            "spark.executor.memory",
            "spark.executor.cores",
            "spark.executor.instances",
            "spark.dynamicAllocation.enabled",
            "spark.executor.extraJavaOptions",
            "spark.eventLog.enabled",
            "spark.eventLog.dir",
            "spark.serializer",
            "spark.kryo.classesToRegister",
            "spark.kryo.registrator",
            "spark.local.dir",
            "spark.logConf",
            "spark.driver.host",
            "spark.driver.port",
            "spark.driver.maxResultSize",
            "spark.ui.showConsoleProgress",
            "spark.io.compression.codec",
            "spark.driver.memory",
            "spark.executor.heartbeatInterval",
            "spark.network.timeout",
    };

    private static final String[] OPTIONAL_HADOOP_PROPERTIES = {
        "fs.s3.awsAccessKeyId",
        "fs.s3.awsSecretAccessKey"
    };

    /**
     * <i>Lazy-initialized.</i> Maintains a reference to a {@link JavaSparkContext}. This instance's reference, however,
     * does not hold a counted reference, so it might be disposed.
     */
    private SparkContextReference sparkContextReference;

    private Logger logger = LogManager.getLogger(this.getClass());

    public static SparkPlatform getInstance() {
        if (instance == null) {
            instance = new SparkPlatform();
        }
        return instance;
    }

    private SparkPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    /**
     * Configures the single maintained {@link JavaSparkContext} according to the {@code job} and returns it.
     *
     * @return a {@link SparkContextReference} wrapping the {@link JavaSparkContext}
     */
    public SparkContextReference getSparkContext(Job job) {

        // NB: There must be only one JavaSparkContext per JVM. Therefore, it is not local to the executor.
        final SparkConf sparkConf;
        final Configuration configuration = job.getConfiguration();
        if (this.sparkContextReference != null && !this.sparkContextReference.isDisposed()) {
            final JavaSparkContext sparkContext = this.sparkContextReference.get();
            this.logger.warn(
                    "There is already a SparkContext (master: {}): , which will be reused. " +
                            "Not all settings might be effective.", sparkContext.getConf().get("spark.master"));
            sparkConf = sparkContext.getConf();

        } else {
            sparkConf = new SparkConf(true);
        }

        for (String property : REQUIRED_SPARK_PROPERTIES) {
            sparkConf.set(property, configuration.getStringProperty(property));
        }
        for (String property : OPTIONAL_SPARK_PROPERTIES) {
            configuration.getOptionalStringProperty(property).ifPresent(
                    value -> sparkConf.set(property, value)
            );
        }

        if (job.getName() != null) {
            sparkConf.set("spark.app.name", job.getName());
        }

        if (this.sparkContextReference == null || this.sparkContextReference.isDisposed()) {
            this.sparkContextReference = new SparkContextReference(job.getCrossPlatformExecutor(), new JavaSparkContext(sparkConf));
        }
        final JavaSparkContext sparkContext = this.sparkContextReference.get();

        org.apache.hadoop.conf.Configuration hadoopconf = sparkContext.hadoopConfiguration();
        for (String property: OPTIONAL_HADOOP_PROPERTIES){
            System.out.println(property);
            configuration.getOptionalStringProperty(property).ifPresent(
                value -> hadoopconf.set(property, value)
            );
        }

        // Set up the JAR files.
        //sparkContext.clearJars();
        if (!sparkContext.isLocal()) {
            // Add Wayang JAR files.
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(SparkPlatform.class)); // wayang-spark
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(WayangBasic.class)); // wayang-basic
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(WayangContext.class)); // wayang-core
            final Set<String> udfJarPaths = job.getUdfJarPaths();
            if (udfJarPaths.isEmpty()) {
                this.logger.warn("Non-local SparkContext but not UDF JARs have been declared.");
            } else {
                udfJarPaths.forEach(this::registerJarIfNotNull);
            }
        }

        return this.sparkContextReference;
    }

    private void registerJarIfNotNull(String path) {
        if (path != null) this.sparkContextReference.get().addJar(path);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("wayang.spark.cpu.mhz");
        int numMachines = (int) configuration.getLongProperty("wayang.spark.machines");
        int numCores = (int) (numMachines * configuration.getLongProperty("wayang.spark.cores-per-machine"));
        double hdfsMsPerMb = configuration.getDoubleProperty("wayang.spark.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("wayang.spark.network.ms-per-mb");
        double stretch = configuration.getDoubleProperty("wayang.spark.stretch");
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
                configuration.getDoubleProperty("wayang.spark.costs.fix"),
                configuration.getDoubleProperty("wayang.spark.costs.per-ms")
        );
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new SparkExecutor(this, job);
    }

    @Override
    public void warmUp(Configuration configuration) {
        super.warmUp(configuration);

        // Run a most simple Spark job.
        this.logger.info("Running warm-up Spark job...");
        long startTime = System.currentTimeMillis();
        final WayangContext wayangCtx = new WayangContext(configuration);
        SparkCollectionSource<Integer> source = new SparkCollectionSource<>(
                Collections.singleton(0), DataSetType.createDefault(Integer.class)
        );
        SparkLocalCallbackSink<Integer> sink = new SparkLocalCallbackSink<>(
                dq -> {
                },
                DataSetType.createDefault(Integer.class)
        );
        source.connectTo(0, sink, 0);
        final Job job = wayangCtx.createJob("Warm up", new WayangPlan(sink));
        // Make sure not to have the warm-up jobs bloat the execution logs.
        job.getConfiguration().setProperty("wayang.core.log.enabled", "false");
        job.execute();
        long stopTime = System.currentTimeMillis();
        this.logger.info("Spark warm-up finished in {}.", Formats.formatDuration(stopTime - startTime, true));

    }

    @Override
    public long getInitializeMillis(Configuration configuration) {
        return configuration.getLongProperty(INITIALIZATION_MS_CONFIG_KEY);
    }
}
