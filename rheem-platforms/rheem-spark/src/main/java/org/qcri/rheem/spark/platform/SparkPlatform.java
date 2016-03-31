package org.qcri.rheem.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.channels.SparkChannelManager;
import org.qcri.rheem.spark.mapping.*;
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
     * Configures the single maintained {@link JavaSparkContext} according to the {@code configuration} and returns it.
     */
    public JavaSparkContext getSparkContext(Configuration configuration) {
        // NB: There must be only one JavaSparkContext per JVM. Therefore, it is not local to the executor.
        if (this.sparkContext != null) {
            LoggerFactory.getLogger(this.getClass()).warn("There is already a SparkContext, which will be reused.");
            return this.sparkContext;
        }

        final SparkConf sparkConf = new SparkConf(true);
        final String master = configuration.getStringProperty("spark.master");
        sparkConf.setMaster(master);
        final String appName = configuration.getStringProperty("spark.appName");
        sparkConf.setAppName(appName);
        configuration.getOptionalStringProperty("spark.jars").ifPresent(
                sparkJars -> sparkConf.setJars(sparkJars.split(","))
        );
        configuration.getOptionalStringProperty("spark.executor.memory").ifPresent(
                mem -> sparkConf.set("spark.executor.memory", mem)
        );
        configuration.getOptionalStringProperty("spark.driver.cores").ifPresent(
                cores -> sparkConf.set("spark.driver.cores", cores)
        );
        configuration.getOptionalStringProperty("spark.driver.memory").ifPresent(
                cores -> sparkConf.set("spark.driver.memory", cores)
        );

        return this.sparkContext = new JavaSparkContext(sparkConf);
    }

    void closeSparkContext(JavaSparkContext sc) {
        if (this.sparkContext == sc) {
            this.sparkContext.close();
            this.sparkContext = null;
        }
    }

    private void initializeConfiguration() {
        String configFileUrl = this.getClass().getResource(DEFAULT_CONFIG_FILE).toString();
        Configuration.getDefaultConfiguration().load(configFileUrl);
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
        return configuration -> new SparkExecutor(this, configuration);
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
