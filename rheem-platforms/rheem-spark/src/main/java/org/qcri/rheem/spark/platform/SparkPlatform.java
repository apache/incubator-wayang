package org.qcri.rheem.spark.platform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.channels.SparkChannelManager;
import org.qcri.rheem.spark.mapping.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class SparkPlatform extends Platform {

    private static final String PLATFORM_NAME = "Apache Spark";

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
        this.initializeMappings();
    }

    public JavaSparkContext getSparkContext() {
        Properties default_properties = new Properties();
        default_properties.setProperty("spark.appName", "rheem");
        default_properties.setProperty("spark.master", "local");
        Properties properties = new Properties(default_properties);
        try {
            properties.load(new FileReader(new File("app.properties")));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not find app.properties file, using default local spark configuration.");
        }
        // NB: There must be only one JavaSparkContext per JVM. Therefore, it is not local to the executor.
        if (this.sparkContext == null) {
            String appName = properties.getProperty("spark.appName");
            String master = properties.getProperty("spark.master");
            final SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
            this.sparkContext = new JavaSparkContext(conf);
        }
        return this.sparkContext;
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
        return () -> new SparkExecutor(this);
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
