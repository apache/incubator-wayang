package org.apache.incubator.wayang.flink.platform;

import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.incubator.wayang.basic.plugin.WayangBasic;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.api.Job;
import org.apache.incubator.wayang.core.api.WayangContext;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.incubator.wayang.core.platform.Executor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.util.ReflectionUtils;
import org.apache.incubator.wayang.flink.execution.FlinkContextReference;
import org.apache.incubator.wayang.flink.execution.FlinkExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Configuration conf = job.getConfiguration();
        if(this.flinkContextReference == null)
            switch (conf.getStringProperty("wayang.flink.mode.run")) {
            case "local":
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.createLocalEnvironment(),
                        (int)conf.getLongProperty("wayang.flink.paralelism")
                );
                break;
            case "distribution":
                String[] jars = getJars(job);
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.createRemoteEnvironment(
                                conf.getStringProperty("wayang.flink.master"),
                                Integer.parseInt(conf.getStringProperty("wayang.flink.port")),
                                jars
                        ),
                        (int)conf.getLongProperty("wayang.flink.paralelism")
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
        int numCores = (int) ( configuration.getLongProperty("wayang.flink.paralelism"));
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
        List<String> jars = new ArrayList<>(5);
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
