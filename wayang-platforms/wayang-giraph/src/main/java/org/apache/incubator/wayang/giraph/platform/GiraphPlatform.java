package org.apache.incubator.wayang.giraph.platform;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.incubator.wayang.core.platform.Executor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.util.ReflectionUtils;
import org.apache.incubator.wayang.giraph.execution.GiraphExecutor;

/**
 * Giraph {@link Platform} for Wayang.
 */
public class GiraphPlatform extends Platform{
    public static final String CPU_MHZ_PROPERTY = "wayang.giraph.cpu.mhz";

    public static final String CORES_PROPERTY = "wayang.giraph.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "wayang.giraph.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "wayang-giraph-defaults.properties";

    private static GiraphPlatform instance;



    protected GiraphPlatform() {
        super("Giraph", "giraph");
        this.initialize();
    }

    /**
     * Initializes this instance.
     */
    private void initialize() {

    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    public static GiraphPlatform getInstance() {
        if (instance == null) {
            instance = new GiraphPlatform();
        }
        return instance;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new GiraphExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(CPU_MHZ_PROPERTY);
        int numCores = (int) configuration.getLongProperty(CORES_PROPERTY);
        double hdfsMsPerMb = configuration.getDoubleProperty(HDFS_MS_PER_MB_PROPERTY);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.giraph.costs.fix"),
                configuration.getDoubleProperty("wayang.giraph.costs.per-ms")
        );
    }
}
