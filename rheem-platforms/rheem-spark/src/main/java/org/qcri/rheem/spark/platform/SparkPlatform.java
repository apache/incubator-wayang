package org.qcri.rheem.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.basic.plugin.RheemBasicPlatform;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.spark.channels.ChannelConversions;
import org.qcri.rheem.spark.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class SparkPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Spark";

    private static final String DEFAULT_CONFIG_FILE = "rheem-spark-defaults.properties";

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static SparkPlatform instance = null;

    private static final String[] REQUIRED_SPARK_PROPERTIES = {
            "spark.master",
            "spark.app.name"
    };

    private static final String[] OPTIONAL_SPARK_PROPERTIES = {
            "spark.executor.memory",
            "spark.executor.extraJavaOptions",
            "spark.eventLog.enabled",
            "spark.eventLog.dir",
            "spark.serializer",
            "spark.kryo.classesToRegister",
            "spark.kryo.registrator",
            "spark.local.dir",
            "spark.logConf",
            "spark.driver.host",
            "spark.driver.port"
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
        super(PLATFORM_NAME);
        this.initializeConfiguration();
        this.initializeMappings();
    }

    @Override
    public void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph) {
        ChannelConversions.ALL.forEach(channelConversionGraph::add);
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

        if (this.sparkContextReference == null || this.sparkContextReference.isDisposed()) {
            this.sparkContextReference = new SparkContextReference(job.getCrossPlatformExecutor(), new JavaSparkContext(sparkConf));
        }
        final JavaSparkContext sparkContext = this.sparkContextReference.get();

        // Set up the JAR files.
        sparkContext.clearJars();
        if (!sparkContext.isLocal()) {
            // Add Rheem JAR files.
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(SparkPlatform.class)); // rheem-spark
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(RheemBasicPlatform.class)); // rheem-basic
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

    private void initializeConfiguration() {
        Configuration.getDefaultConfiguration().load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    private void initializeMappings() {
        this.mappings.add(new CartesianToSparkCartesianMapping());
        this.mappings.add(new CollectionSourceMapping());
        this.mappings.add(new CountToSparkCountMapping());
        this.mappings.add(new DistinctToSparkDistinctMapping());
        this.mappings.add(new FilterToSparkFilterMapping());
        this.mappings.add(new GlobalReduceMapping());
        this.mappings.add(new GlobalMaterializedGroupToSparkGlobalMaterializedGroupMapping());
        this.mappings.add(new LocalCallbackSinkMapping());
        this.mappings.add(new FlatMapToSparkFlatMapMapping());
        this.mappings.add(new MapOperatorToSparkMapOperatorMapping());
        this.mappings.add(new MapOperatorToSparkMapPartitionsOperatorMapping());
        this.mappings.add(new MtrlGroupByToSparkMtrlGroupByMapping());
        this.mappings.add(new ReduceByToSparkReduceByMapping());
        this.mappings.add(new SortToSparkSortMapping());
        this.mappings.add(new TextFileSourceMapping());
        this.mappings.add(new UnionAllToSparkUnionAllMapping());
        this.mappings.add(new LoopToSparkLoopMapping());
        this.mappings.add(new DoWhileMapping());
        this.mappings.add(new SampleToSparkSampleMapping());
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("rheem.spark.cpu.mhz");
        int numMachines = (int) configuration.getLongProperty("rheem.spark.machines");
        int numCores = (int) (numMachines * configuration.getLongProperty("rheem.spark.cores-per-machine"));
        double hdfsMsPerMb = configuration.getDoubleProperty("rheem.spark.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("rheem.spark.network.ms-per-mb");
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(networkMsPerMb / 1000000d),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new SparkExecutor(this, job);
    }

}
