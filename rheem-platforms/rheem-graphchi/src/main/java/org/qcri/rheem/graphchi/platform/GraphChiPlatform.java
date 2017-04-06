package org.qcri.rheem.graphchi.platform;

import edu.cmu.graphchi.io.CompressedIO;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;

/**
 * GraphChi {@link Platform} for Rheem.
 */
public class GraphChiPlatform extends Platform {

    public static final String CPU_MHZ_PROPERTY = "rheem.graphchi.cpu.mhz";

    public static final String CORES_PROPERTY = "rheem.graphchi.cores";

    public static final String HDFS_MS_PER_MB_PROPERTY = "rheem.graphchi.hdfs.ms-per-mb";

    private static final String DEFAULT_CONFIG_FILE = "rheem-graphchi-defaults.properties";

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
                configuration.getDoubleProperty("rheem.graphchi.costs.fix"),
                configuration.getDoubleProperty("rheem.graphchi.costs.per-ms")
        );
    }
}
