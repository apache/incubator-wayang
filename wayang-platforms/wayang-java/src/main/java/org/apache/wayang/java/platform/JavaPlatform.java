package org.apache.incubator.wayang.java.platform;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.incubator.wayang.core.platform.Executor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.util.ReflectionUtils;
import org.apache.incubator.wayang.java.execution.JavaExecutor;

/**
 * {@link Platform} for a single JVM executor based on the {@link java.util.stream} library.
 */
public class JavaPlatform extends Platform {

    private static final String PLATFORM_NAME = "Java Streams";

    private static final String CONFIG_NAME = "java";

    private static final String DEFAULT_CONFIG_FILE = "wayang-java-defaults.properties";

    private static JavaPlatform instance = null;

    public static JavaPlatform getInstance() {
        if (instance == null) {
            instance = new JavaPlatform();
        }
        return instance;
    }

    private JavaPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JavaExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty("wayang.java.cpu.mhz");
        int numCores = (int) configuration.getLongProperty("wayang.java.cores");
        double hdfsMsPerMb = configuration.getDoubleProperty("wayang.java.hdfs.ms-per-mb");
        double stretch = configuration.getDoubleProperty("wayang.java.stretch");
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.java.costs.fix"),
                configuration.getDoubleProperty("wayang.java.costs.per-ms")
        );
    }
}
