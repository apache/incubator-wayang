package org.qcri.rheem.flink.platform;

import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
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
import org.qcri.rheem.flink.execution.FlinkContextReference;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * {@link Platform} for Apache Spark.
 */
public class FlinkPlatform extends Platform {
    private static final String PLATFORM_NAME = "Apache Flink";

    private static final String CONFIG_NAME = "flink";

    private static final String DEFAULT_CONFIG_FILE = "rheem-flink-defaults.properties";

    public static final String INITIALIZATION_MS_CONFIG_KEY = "rheem.flink.init.ms";

    private static FlinkPlatform instance = null;

    private static final String[] REQUIRED_FLINK_PROPERTIES = {
    };

    private static final String[] OPTIONAL_FLINK_PROPERTIES = {

    };

    /**
     * <i>Lazy-initialized.</i> Maintains a reference to a {@link ExecutionEnvironment}. This instance's reference, however,
     * does not hold a counted reference, so it might be disposed.
     */
    private FlinkContextReference flinkContextReference;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

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
        //load of parameters HERE!!!!1
        Configuration conf = job.getConfiguration();
        if(this.flinkContextReference == null) {

            switch (conf.getStringProperty("rheem.flink.mode.run")) {
                case "local":
                    this.flinkContextReference = new FlinkContextReference(
                                                        job.getCrossPlatformExecutor(),
                                                        ExecutionEnvironment.createLocalEnvironment()
                                                );
                    break;
                case "distribution":
                    this.flinkContextReference = new FlinkContextReference(
                                                        job.getCrossPlatformExecutor(),
                                                        ExecutionEnvironment.getExecutionEnvironment()
                                                );
                    break;
                case "collection":
                default:
                    this.flinkContextReference = new FlinkContextReference(
                                                        job.getCrossPlatformExecutor(),
                                                        new CollectionEnvironment()
                                                );
                    break;
            }
        }

        return this.flinkContextReference;

    }

    private void registerJarIfNotNull(String path) {
        if (path != null);
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
        return null;
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return null;
    }


}
