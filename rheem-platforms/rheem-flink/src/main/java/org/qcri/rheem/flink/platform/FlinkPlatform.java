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
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.flink.execution.FlinkContextReference;
import org.qcri.rheem.flink.execution.FlinkExecutor;
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
            switch (conf.getStringProperty("rheem.flink.mode.run")) {
            case "local":
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.createLocalEnvironment(),
                        (int)conf.getLongProperty("rheem.flink.paralelism")
                );
                break;
            case "distribution":
                String[] jars = getJars(job);
                this.flinkContextReference = new FlinkContextReference(
                        job.getCrossPlatformExecutor(),
                        ExecutionEnvironment.createRemoteEnvironment(
                                conf.getStringProperty("rheem.flink.master"),
                                Integer.parseInt(conf.getStringProperty("rheem.flink.port")),
                                jars
                        ),
                        (int)conf.getLongProperty("rheem.flink.paralelism")
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
        int cpuMhz = (int) configuration.getLongProperty("rheem.flink.cpu.mhz");
        int numCores = (int) ( configuration.getLongProperty("rheem.flink.paralelism"));
        double hdfsMsPerMb = configuration.getDoubleProperty("rheem.flink.hdfs.ms-per-mb");
        double networkMsPerMb = configuration.getDoubleProperty("rheem.flink.network.ms-per-mb");
        double stretch = configuration.getDoubleProperty("rheem.flink.stretch");
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
                configuration.getDoubleProperty("rheem.flink.costs.fix"),
                configuration.getDoubleProperty("rheem.flink.costs.per-ms")
        );
    }


    private String[] getJars(Job job){
        List<String> jars = new ArrayList<>(5);
        List<Class> clazzs = Arrays.asList(new Class[]{FlinkPlatform.class, RheemBasic.class, RheemContext.class});

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
