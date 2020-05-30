package org.qcri.rheem.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.basic.plugin.RheemBasic;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.spark.execution.SparkContextReference;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.operators.SparkCollectionSource;
import org.qcri.rheem.spark.operators.SparkLocalCallbackSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * {@link Platform} for Apache Spark.
 */
public class SparkPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Spark";

    private static final String CONFIG_NAME = "spark";

    private static final String DEFAULT_CONFIG_FILE = "rheem-spark-defaults.properties";

    public static final String INITIALIZATION_MS_CONFIG_KEY = "rheem.spark.init.ms";

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
            "spark.network.timeout"
    };

    /**
     * <i>Lazy-initialized.</i> Maintains a reference to a {@link JavaSparkContext}. This instance's reference, however,
     * does not hold a counted reference, so it might be disposed.
     */
    private SparkContextReference sparkContextReference;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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

        // Set up the JAR files.
        //sparkContext.clearJars();
        if (!sparkContext.isLocal()) {
            // Add Rheem JAR files.
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(SparkPlatform.class)); // rheem-spark
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(RheemBasic.class)); // rheem-basic
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(RheemContext.class)); // rheem-core
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
        int cpuMhz = (int) configuration.getLongProperty("rheem.spark.cpu.mhz");
        int numMachines = (int) configuration.getLongProperty("rheem.spark.machines");
        int numCores = (int) (numMachines * configuration.getLongProperty("rheem.spark.cores-per-machine"));
        double hdfsMsPerMb = configuration.getDoubleProperty("rheem.spark.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("rheem.spark.network.ms-per-mb");
        double stretch = configuration.getDoubleProperty("rheem.spark.stretch");
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
                configuration.getDoubleProperty("rheem.spark.costs.fix"),
                configuration.getDoubleProperty("rheem.spark.costs.per-ms")
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
        final RheemContext rheemCtx = new RheemContext(configuration);
        SparkCollectionSource<Integer> source = new SparkCollectionSource<>(
                Collections.singleton(0), DataSetType.createDefault(Integer.class)
        );
        SparkLocalCallbackSink<Integer> sink = new SparkLocalCallbackSink<>(
                dq -> {
                },
                DataSetType.createDefault(Integer.class)
        );
        source.connectTo(0, sink, 0);
        final Job job = rheemCtx.createJob("Warm up", new RheemPlan(sink));
        // Make sure not to have the warm-up jobs bloat the execution logs.
        job.getConfiguration().setProperty("rheem.core.log.enabled", "false");
        job.execute();
        long stopTime = System.currentTimeMillis();
        this.logger.info("Spark warm-up finished in {}.", Formats.formatDuration(stopTime - startTime, true));

    }

    @Override
    public long getInitializeMillis(Configuration configuration) {
        return configuration.getLongProperty(INITIALIZATION_MS_CONFIG_KEY);
    }
}
