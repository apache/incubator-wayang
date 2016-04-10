package org.qcri.rheem.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.basic.plugin.RheemBasicPlatform;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.spark.channels.SparkChannelManager;
import org.qcri.rheem.spark.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class SparkPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Spark";

    private static final String DEFAULT_CONFIG_FILE = "/rheem-spark-defaults.properties";

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static SparkPlatform instance = null;

    /**
     * <i>Lazy-initialized.</i> Allows to create Spark jobs. Is shared among all executors.
     */
    private JavaSparkContext sparkContext;

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

    /**
     * Configures the single maintained {@link JavaSparkContext} according to the {@code job} and returns it.
     */
    public JavaSparkContext getSparkContext(Job job) {

        // NB: There must be only one JavaSparkContext per JVM. Therefore, it is not local to the executor.
        final SparkConf sparkConf;
        if (this.sparkContext != null) {
            this.logger.warn(
                    "There is already a SparkContext (master: {}): , which will be reused. " +
                    "Not all settings might be effective.", this.sparkContext.getConf().get("spark.master"));
            sparkConf = this.sparkContext.getConf();
        } else {
            sparkConf = new SparkConf(true);
        }

        Configuration configuration = job.getConfiguration();
        final String master = configuration.getStringProperty("spark.master");
        sparkConf.setMaster(master);
        final String appName = configuration.getStringProperty("spark.appName");
        sparkConf.setAppName(appName);
        configuration.getOptionalStringProperty("spark.executor.memory").ifPresent(
                mem -> sparkConf.set("spark.executor.memory", mem)
        );
        configuration.getOptionalStringProperty("spark.driver.cores").ifPresent(
                cores -> sparkConf.set("spark.driver.cores", cores)
        );
        configuration.getOptionalStringProperty("spark.driver.memory").ifPresent(
                cores -> sparkConf.set("spark.driver.memory", cores)

        );
        if (this.sparkContext == null) {
            this.sparkContext = new JavaSparkContext(sparkConf);
        }

        // Set up the JAR files.
        this.sparkContext.clearJars();
        if (!this.sparkContext.isLocal()) {
            // Add Rheem JAR files.
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(SparkPlatform.class)); // rheem-spark
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(RheemBasicPlatform.class)); // rheem-basic
            this.registerJarIfNotNull(ReflectionUtils.getDeclaringJar(RheemContext.class)); // rheem-core
            if (job.getUdfJarPaths().isEmpty()) {
                this.logger.warn("Non-local SparkContext but not UDF JARs have been declared.");
            }  else {
                job.getUdfJarPaths().forEach(this::registerJarIfNotNull);
            }
        }

        return this.sparkContext;
    }

    private void registerJarIfNotNull(String path) {
        if (path != null) this.sparkContext.addJar(path);
    }

    void closeSparkContext(JavaSparkContext sc) {
        if (this.sparkContext == sc) {
            this.sparkContext.close();
            this.sparkContext = null;
        }
    }

    private void initializeConfiguration() {
        Configuration.getDefaultConfiguration().load(this.getClass().getResourceAsStream(DEFAULT_CONFIG_FILE));
    }

    private void initializeMappings() {
        this.mappings.add(new CartesianToSparkCartesianMapping());
        this.mappings.add(new CollectionSourceMapping());
        this.mappings.add(new CountToSparkCountMapping());
        this.mappings.add(new DistinctToSparkDistinctMapping());
        this.mappings.add(new FilterToSparkFilterMapping());
        this.mappings.add(new GlobalReduceMapping());
        this.mappings.add(new LocalCallbackSinkMapping());
        this.mappings.add(new FlatMapToSparkFlatMapMapping());
        this.mappings.add(new MapOperatorToSparkMapOperatorMapping());
        this.mappings.add(new MtrlGroupByToSparkMtrlGroupByMapping());
        this.mappings.add(new ReduceByToSparkReduceByMapping());
        this.mappings.add(new SortToSparkSortMapping());
        this.mappings.add(new TextFileSourceMapping());
        this.mappings.add(new UnionAllToSparkUnionAllMapping());
        this.mappings.add(new LoopToSparkLoopMapping());
        this.mappings.add(new DoWhileMapping());
        this.mappings.add(new BernoulliSampleToSparkBernoulliSampleMapping());
        this.mappings.add(new SampleToSparkRandomSampleMapping());
        this.mappings.add(new SampleToSparkShuffleSampleMapping());
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

    @Override
    protected ChannelManager createChannelManager() {
        return new SparkChannelManager(this);
    }

    @Override
    public SparkChannelManager getChannelManager() {
        return (SparkChannelManager) super.getChannelManager();
    }
}
