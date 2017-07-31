package org.qcri.rheem.giraph.platform;

import org.apache.giraph.conf.GiraphConfiguration;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.giraph.execution.GiraphExecutor;

/**
 * Giraph {@link Platform} for Rheem.
 */
public class GiraphPlatform extends Platform{
    public static final String CPU_MHZ_PROPERTY = "rheem.giraph.cpu.mhz";

    public static final String CORES_PROPERTY = "rheem.giraph.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "rheem.giraph.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "rheem-giraph-defaults.properties";

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
                configuration.getDoubleProperty("rheem.giraph.costs.fix"),
                configuration.getDoubleProperty("rheem.giraph.costs.per-ms")
        );
    }
}
