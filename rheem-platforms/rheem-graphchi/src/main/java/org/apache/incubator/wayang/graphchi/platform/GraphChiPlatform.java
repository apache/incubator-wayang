package org.apache.incubator.wayang.graphchi.platform;

import edu.cmu.graphchi.io.CompressedIO;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.incubator.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.incubator.wayang.core.platform.Executor;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.util.ReflectionUtils;
import org.apache.incubator.wayang.graphchi.execution.GraphChiExecutor;

/**
 * GraphChi {@link Platform} for Wayang.
 */
public class GraphChiPlatform extends Platform {

    public static final String CPU_MHZ_PROPERTY = "wayang.graphchi.cpu.mhz";

    public static final String CORES_PROPERTY = "wayang.graphchi.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "wayang.graphchi.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "wayang-graphchi-defaults.properties";

    private static GraphChiPlatform instance;

    protected GraphChiPlatform() {
        super("GraphChi", "graphchi");
        this.initialize();
    }

    /**
     * Initializes this instance.
     */
    private void initialize() {
        // Set up.
        CompressedIO.disableCompression();
        GraphChiPlatform.class.getClassLoader().setClassAssertionStatus(
                "edu.cmu.graphchi.preprocessing.FastSharder", false);
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    public static GraphChiPlatform getInstance() {
        if (instance == null) {
            instance = new GraphChiPlatform();
        }
        return instance;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new GraphChiExecutor(this, job);
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
                configuration.getDoubleProperty("wayang.graphchi.costs.fix"),
                configuration.getDoubleProperty("wayang.graphchi.costs.per-ms")
        );
    }
}
